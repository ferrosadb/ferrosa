//! Module: Bounded execution pool that reserves CPU headroom for consensus.
//!
//! Correctness: correct when no more than `cores - reserved` CPU-bound closures
//! run concurrently — so the raft runtime always has a schedulable core — and
//! every admission slot is returned on completion, drop, or panic (RAII). Unit
//! and stress tests remain green.
//!
//! Last revised: 2026-07-22
//! Last changed: B1 fair scheduling. Added [`submit_scan`](SchedPool::submit_scan)
//!   with [`ScanSlot`] cooperative yielding (a long scan releases and fairly
//!   re-acquires its pool slot every `chunk_budget` chunks so it can't
//!   monopolize the pool), and the [`runqueue`] and [`scheduler`] vruntime
//!   fair-share modules. Phase 0 (t_88223ad0) bounded the `spawn_blocking` pool
//!   so a scan fan-out cannot starve raft; B1 makes concurrent scans share it
//!   fairly.
//!
//! # Design
//!
//! ferrosa already isolates raft at the *runtime* level (a dedicated multi-thread
//! runtime + per-peer OS threads). The residual starvation is the **blocking
//! pool**: storage scan producers `spawn_blocking` with no cap, so a broad scan
//! admits hundreds of blocking tasks that saturate every core. [`SchedPool`]
//! gates admission with a semaphore sized to [`Reservation::available`] so the
//! reserved cores stay free for consensus. This crate is a leaf: it depends only
//! on tokio, so nothing in the storage/cluster/cql stack leaks in (DSM guard).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

pub mod fair_admit;
pub mod group_runqueue;
pub mod io_permits;
pub mod runqueue;
pub mod runtime_monitor;
pub mod scheduler;

pub use fair_admit::Admitted;
pub use group_runqueue::{GroupId, GroupRunQueue, Picked};
pub use io_permits::{IoPermit, IoPermits};

// Process-wide pool metrics. Cumulative counters live here (the Prometheus
// registry reads them); instantaneous gauges (`headroom_cores`, `active`) are
// read live off the global pool.
static TASKS_ADMITTED_TOTAL: AtomicU64 = AtomicU64::new(0);
static ADMIT_WAIT_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Total closures admitted to the bounded pool since process start.
pub fn tasks_admitted_total() -> u64 {
    TASKS_ADMITTED_TOTAL.load(Ordering::Relaxed)
}

/// Cumulative microseconds producers spent waiting for an admission slot
/// (`admit-wait latency`). Rising fast = the pool is the bottleneck (scans
/// queued behind the reservation), which is the intended backpressure.
pub fn admit_wait_micros_total() -> u64 {
    ADMIT_WAIT_MICROS_TOTAL.load(Ordering::Relaxed)
}

fn record_admission(wait: Duration) {
    TASKS_ADMITTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    ADMIT_WAIT_MICROS_TOTAL.fetch_add(wait.as_micros() as u64, Ordering::Relaxed);
}

static ADMISSIONS_REJECTED_OVERLOAD_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Admission requests rejected because the waiter queue was already at its bound
/// (`Admitted::Overloaded`). Non-zero means real overload — more scans arrived
/// than the reservation can queue — and the caller shed them (fail-loud) rather
/// than pile up unboundedly. A rising rate should alert.
pub fn admissions_rejected_overload_total() -> u64 {
    ADMISSIONS_REJECTED_OVERLOAD_TOTAL.load(Ordering::Relaxed)
}

/// Called by [`fair_admit::FairAdmit::admit`] when it sheds a request under
/// overload.
pub(crate) fn record_overload_rejection() {
    ADMISSIONS_REJECTED_OVERLOAD_TOTAL.fetch_add(1, Ordering::Relaxed);
}

static IO_PERMITS_ACQUIRED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Bulk-I/O permits ([`io_permits::IoPermits`]) acquired since process start —
/// the throughput of the I/O resource dimension (B2). Paired with a live
/// `in_flight` gauge once a pool is wired into the Bulk lane.
pub fn io_permits_acquired_total() -> u64 {
    IO_PERMITS_ACQUIRED_TOTAL.load(Ordering::Relaxed)
}

/// Called by [`io_permits::IoPermits`] when a bulk-I/O permit is granted.
pub(crate) fn record_io_permit_acquired() {
    IO_PERMITS_ACQUIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

static SCAN_RELEASES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Times a scan producer gave back its pool slot AND its thread because its
/// consumer was not keeping up ([`ScanSlot::release`]), to resume later from
/// its own position. One per pause; a high rate against a low query rate
/// means clients hold results open without reading them.
pub fn scan_releases_total() -> u64 {
    SCAN_RELEASES_TOTAL.load(Ordering::Relaxed)
}

fn record_scan_release() {
    SCAN_RELEASES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

// Chunk-budget tripwire (B1 T1.3 / FMEA FM-2). A scan chunk is the work between
// two `ScanSlot::tick`s; a chunk longer than the budget is a *yield-point gap* —
// the producer held the pool slot too long without a chance to cede it, which
// reintroduces the monopolization B1 fixes.
static SCHED_MAX_CHUNK_MICROS: AtomicU64 = AtomicU64::new(0);
static SCHED_OVER_BUDGET_CHUNKS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Per-chunk work-time budget in microseconds (default 50 ms). A scan chunk
/// longer than this trips [`over_budget_chunks_total`] — an FM-2 signal that a
/// producer needs to chunk more finely (e.g. a pathologically large partition
/// decoded without an intervening yield).
pub const DEFAULT_CHUNK_BUDGET_MICROS: u64 = 50_000;

/// Longest scan chunk observed (work between cooperative yields), microseconds.
pub fn sched_max_chunk_micros() -> u64 {
    SCHED_MAX_CHUNK_MICROS.load(Ordering::Relaxed)
}

/// Count of scan chunks that exceeded [`DEFAULT_CHUNK_BUDGET_MICROS`]. Non-zero
/// in steady state means a scan producer is holding the pool slot too long
/// between yields and should alert.
pub fn over_budget_chunks_total() -> u64 {
    SCHED_OVER_BUDGET_CHUNKS_TOTAL.load(Ordering::Relaxed)
}

fn record_chunk(elapsed: Duration) {
    let micros = elapsed.as_micros() as u64;
    SCHED_MAX_CHUNK_MICROS.fetch_max(micros, Ordering::Relaxed);
    if micros > DEFAULT_CHUNK_BUDGET_MICROS {
        SCHED_OVER_BUDGET_CHUNKS_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
}

/// Render the scheduler's Prometheus metrics (text exposition format), reading
/// the live gauges off the process-global pool plus the cumulative counters.
/// Concatenated into `/metrics` by the web layer.
///
/// `ferrosa_sched_consensus_headroom_cores` is the load-bearing gauge for
/// `t_88223ad0`: it must stay `> 0` (indeed `>= reserved`) throughout a scan
/// storm — if it hits 0, consensus has no schedulable core.
pub fn render_prometheus() -> String {
    let pool = global_pool();
    let mut out = String::with_capacity(768);
    out.push_str(
        "# HELP ferrosa_sched_consensus_headroom_cores CPU cores currently free of scheduler-pool work (>= reserved).\n\
         # TYPE ferrosa_sched_consensus_headroom_cores gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_consensus_headroom_cores {}\n",
        pool.headroom_cores()
    ));
    out.push_str(
        "# HELP ferrosa_sched_pool_capacity Max concurrent background closures the pool admits (cores - reserved).\n\
         # TYPE ferrosa_sched_pool_capacity gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_pool_capacity {}\n",
        pool.capacity()
    ));
    out.push_str(
        "# HELP ferrosa_sched_pool_active Background closures currently admitted (running).\n\
         # TYPE ferrosa_sched_pool_active gauge\n",
    );
    out.push_str(&format!("ferrosa_sched_pool_active {}\n", pool.active()));
    out.push_str(
        "# HELP ferrosa_sched_tasks_admitted_total Closures admitted to the bounded pool since start.\n\
         # TYPE ferrosa_sched_tasks_admitted_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_tasks_admitted_total {}\n",
        tasks_admitted_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_admit_wait_micros_total Cumulative microseconds producers waited for a pool slot.\n\
         # TYPE ferrosa_sched_admit_wait_micros_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_admit_wait_micros_total {}\n",
        admit_wait_micros_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_max_chunk_micros Longest scan chunk (work between cooperative yields), microseconds.\n\
         # TYPE ferrosa_sched_max_chunk_micros gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_max_chunk_micros {}\n",
        sched_max_chunk_micros()
    ));
    out.push_str(
        "# HELP ferrosa_sched_over_budget_chunks_total Scan chunks exceeding the per-chunk work budget (FM-2 yield-point gap); non-zero should alert.\n\
         # TYPE ferrosa_sched_over_budget_chunks_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_over_budget_chunks_total {}\n",
        over_budget_chunks_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_admissions_rejected_overload_total Admission requests shed because the waiter queue was at its bound; non-zero means real overload backpressure.\n\
         # TYPE ferrosa_sched_admissions_rejected_overload_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_admissions_rejected_overload_total {}\n",
        admissions_rejected_overload_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_io_permits_acquired_total Bulk-I/O permits granted since start (the I/O resource dimension, B2).\n\
         # TYPE ferrosa_sched_io_permits_acquired_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_io_permits_acquired_total {}\n",
        io_permits_acquired_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_io_permits_capacity Max concurrent bulk-I/O permits (the Lane::Bulk reservation).\n\
         # TYPE ferrosa_sched_io_permits_capacity gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_io_permits_capacity {}\n",
        pool.io_permits().capacity()
    ));
    out.push_str(
        "# HELP ferrosa_sched_io_permits_in_flight Bulk-I/O permits currently held (in-flight bulk I/O).\n\
         # TYPE ferrosa_sched_io_permits_in_flight gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_io_permits_in_flight {}\n",
        pool.io_permits().in_flight()
    ));
    out.push_str(
        "# HELP ferrosa_sched_scan_carrier_threads_busy Scheduler-owned threads currently running a scan producer.\n\
         # TYPE ferrosa_sched_scan_carrier_threads_busy gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_scan_carrier_threads_busy {}\n",
        scan_carrier_threads_busy()
    ));
    out.push_str(
        "# HELP ferrosa_sched_scan_carrier_threads_capacity Most scheduler-owned threads scan producers can occupy.\n\
         # TYPE ferrosa_sched_scan_carrier_threads_capacity gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_scan_carrier_threads_capacity {}\n",
        scan_carrier_threads_capacity()
    ));
    out.push_str(
        "# HELP ferrosa_sched_scan_releases_total Times a scan producer paused, giving back its pool slot and its thread, because its consumer stopped reading.\n\
         # TYPE ferrosa_sched_scan_releases_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_scan_releases_total {}\n",
        scan_releases_total()
    ));
    // Runtime-stall detector (Phase 3): async-runtime freeze visibility. Kept as
    // its own module (process-global counters, no dependence on the pool) so it
    // reports even before any scan runs.
    runtime_monitor::render_prometheus(&mut out);
    out
}

/// Scheduling class of a scan. Seeds the fair-share weight the live
/// [`fair_admit::FairAdmit`] scheduler orders admission by: a `Foreground` scan
/// (weight 1024) advances `vruntime` 4x slower than a `Bulk` scan (256), so it
/// gets ~4x the slot turns under contention. Consensus work never flows through
/// [`SchedPool`] — it runs on its own reserved runtime — so it has no variant
/// here by design.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedClass {
    /// Latency-sensitive client reads/writes.
    Foreground,
    /// Bulk background work: full-table scans, compaction, index builds.
    Bulk,
}

/// A core reservation: `cores` total, `reserved` kept free for consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    /// Total CPU cores available to the process.
    pub cores: usize,
    /// Cores reserved for consensus (raft heartbeats/elections) — never admitted
    /// to the bulk pool.
    pub reserved: usize,
}

impl Reservation {
    /// Reserve `reserved` of `cores` cores for consensus.
    pub fn new(cores: usize, reserved: usize) -> Self {
        Self { cores, reserved }
    }

    /// Derive the reservation from the detected parallelism, keeping `reserved`
    /// cores free for consensus. Falls back to a single core if detection fails.
    pub fn from_available_parallelism(reserved: usize) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        Self::new(cores, reserved)
    }

    /// CPU-bound background slots after reserving consensus headroom. Always at
    /// least 1 so the pool can make progress even when `reserved >= cores`.
    pub fn available(&self) -> usize {
        self.cores.saturating_sub(self.reserved).max(1)
    }
}

/// Bounded, vruntime-fair execution pool for CPU-bound scan work.
///
/// Admission is governed by [`fair_admit::FairAdmit`]: at most
/// [`capacity`](SchedPool::capacity) = `cores − reserved` scans hold a slot at
/// once (so the reserved consensus cores are never oversubscribed), and a freed
/// slot is granted to the least-`vruntime` waiter — weighted by
/// [`SchedClass`], so a Foreground scan gets ~4x the slot
/// turns of a Bulk scan under contention. B1.5 made this the live authority
/// (replacing the earlier FIFO semaphore).
#[derive(Clone)]
pub struct SchedPool {
    admit: Arc<fair_admit::FairAdmit>,
    capacity: usize,
    reservation: Reservation,
    /// The I/O resource dimension (B2): a bounded set of bulk-I/O permits an
    /// admitted scan holds while it produces (the `Lane::Bulk` reservation).
    /// Sized to `capacity` by default (1:1 with CPU slots — non-constraining);
    /// lowering it reserves I/O concurrency for the other lanes. Cloneable — it
    /// shares one semaphore.
    io_permits: io_permits::IoPermits,
}

/// Releases a held slot on drop — the RAII backstop so a panicking task never
/// leaks its admission slot.
struct SlotGuard {
    admit: Arc<fair_admit::FairAdmit>,
    id: u64,
}
impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.admit.finish(self.id);
    }
}

impl SchedPool {
    /// Build a pool admitting `reservation.available()` scans at once, and
    /// queuing at most [`DEFAULT_MAX_WAITERS_PER_SLOT`] × capacity more before
    /// shedding further requests as [`ScanOutcome::Overloaded`] (bounded
    /// backpressure).
    pub fn new(reservation: Reservation) -> Self {
        let capacity = reservation.available();
        let max_waiters = capacity.saturating_mul(DEFAULT_MAX_WAITERS_PER_SLOT);
        Self {
            admit: Arc::new(fair_admit::FairAdmit::new(capacity, 0, max_waiters)),
            capacity,
            reservation,
            io_permits: io_permits::IoPermits::new(capacity),
        }
    }

    /// The bulk-I/O permit pool (the B2 I/O dimension). A scan submitted via
    /// [`submit_scan`](Self::submit_scan) holds one permit while it produces;
    /// bulk-I/O seams can also acquire from it directly.
    pub fn io_permits(&self) -> &io_permits::IoPermits {
        &self.io_permits
    }

    /// Maximum number of scans admitted concurrently.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Currently free admission slots (`capacity` minus in-flight).
    pub fn available_permits(&self) -> usize {
        self.capacity.saturating_sub(self.admit.active())
    }

    /// Scans currently admitted (holding a slot).
    pub fn active(&self) -> usize {
        self.admit.active()
    }

    /// CPU cores currently NOT doing pool work — the consensus headroom
    /// (`SCHED_CONSENSUS_HEADROOM_CORES`). Never drops below `reserved`, so raft
    /// always has a schedulable core. Falls as scans admit, recovers as they end.
    pub fn headroom_cores(&self) -> usize {
        self.reservation.cores.saturating_sub(self.active())
    }

    /// Admit and run `f` on a blocking thread once a slot is granted (at `Bulk`
    /// weight). Awaiting resolves once admitted; the returned handle resolves to
    /// `Some(f`'s result`)`, or `None` if the request was shed under overload
    /// (the waiter queue was at its bound). The slot is released on return OR
    /// unwind (RAII). Generic entry — the scan producers use
    /// [`submit_scan`](Self::submit_scan).
    pub async fn submit<F, R>(&self, f: F) -> JoinHandle<Option<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let waited = Instant::now();
        // No cancellation signal for the generic entry — pass a never-firing
        // future; overload is still surfaced as `None`.
        match self
            .admit
            .admit(
                DEFAULT_TENANT_GROUP,
                group_runqueue::DEFAULT_GROUP_WEIGHT,
                SchedClass::Bulk,
                std::future::pending::<()>(),
            )
            .await
        {
            Admitted::Slot(id) => {
                record_admission(waited.elapsed());
                let admit = self.admit.clone();
                tokio::task::spawn_blocking(move || {
                    let _guard = SlotGuard { admit, id };
                    Some(f())
                })
            }
            Admitted::Overloaded => tokio::task::spawn_blocking(|| None),
            Admitted::Cancelled => unreachable!("submit passes a never-firing cancel"),
        }
    }

    /// Sync entry for a generic blocking task (fires a cheap async admitter, then
    /// moves `f` onto a blocking thread once admitted — so excess tasks wait as
    /// async tasks, not parked blocking threads). Admits at `Bulk` weight;
    /// resolves to `None` if shed under overload.
    pub fn submit_blocking<F, R>(&self, f: F) -> JoinHandle<Option<R>>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let admit = self.admit.clone();
        tokio::spawn(async move {
            let waited = Instant::now();
            match admit
                .admit(
                    DEFAULT_TENANT_GROUP,
                    group_runqueue::DEFAULT_GROUP_WEIGHT,
                    SchedClass::Bulk,
                    std::future::pending::<()>(),
                )
                .await
            {
                Admitted::Slot(id) => {
                    record_admission(waited.elapsed());
                    tokio::task::spawn_blocking(move || {
                        let _guard = SlotGuard { admit, id };
                        Some(f())
                    })
                    .await
                    .expect("scheduler blocking task must not be cancelled")
                }
                Admitted::Overloaded => None,
                Admitted::Cancelled => unreachable!("submit_blocking passes a never-firing cancel"),
            }
        })
    }

    /// Sync entry for a *cooperative* scan producer (B1). `f` receives a
    /// [`ScanSlot`] and must call [`ScanSlot::tick`] after each produced chunk;
    /// every `chunk_budget` ticks the scan's `vruntime` is charged, and when a
    /// more deserving scan waits the slot is given up ([`Tick::Yield`]) and the
    /// producer must return. `class` seeds the fair-share weight.
    ///
    /// A producer NEVER waits on its thread — not for its consumer, not for its
    /// slot to come back. When it must stop (its consumer is behind:
    /// [`ScanSlot::release`]; or it yielded) it returns, and resumes in a new
    /// run admitted with [`resubmit_scan`](Self::resubmit_scan) and the slot's
    /// [`ScanTicket`]. Every wait is an async task, so no producer holds a
    /// thread that something else needs.
    ///
    /// `cancel` is the consumer-gone signal: when it fires *before admission*,
    /// the scan is dropped from the queue without ever occupying a slot (a
    /// dropped range stream no longer leaves a queued admission that later wakes,
    /// grabs a slot, and does work just to find the closed channel). Pass the
    /// consumer channel's closed-future; pass [`std::future::pending()`] if there
    /// is nothing to cancel on. The returned handle resolves to a
    /// [`ScanOutcome`]: `Ran(f`'s result`)`, `Overloaded` (shed — the producer
    /// never ran; the caller must fail loud), or `Cancelled`.
    pub fn submit_scan<F, R, C>(
        &self,
        class: SchedClass,
        chunk_budget: u32,
        cancel: C,
        f: F,
    ) -> JoinHandle<ScanOutcome<R>>
    where
        F: FnOnce(&mut ScanSlot) -> R + Send + 'static,
        R: Send + 'static,
        C: Future<Output = ()> + Send + 'static,
    {
        let admit = self.admit.clone();
        let io_permits = self.io_permits.clone();
        tokio::spawn(async move {
            let waited = Instant::now();
            let admitted = admit
                .admit(
                    DEFAULT_TENANT_GROUP,
                    group_runqueue::DEFAULT_GROUP_WEIGHT,
                    class,
                    cancel,
                )
                .await;
            run_admitted(admitted, waited, admit, io_permits, chunk_budget, f).await
        })
    }

    /// Run the next stretch of a scan that stopped with a [`ScanTicket`] (it
    /// paused for its consumer, or yielded at a budget boundary). It re-enters
    /// the queue with the id and `vruntime` it left with, so stopping does not
    /// reset its fair share, and it is not shed under overload: it was admitted
    /// once. Otherwise as [`submit_scan`](Self::submit_scan).
    pub fn resubmit_scan<F, R, C>(
        &self,
        ticket: ScanTicket,
        cancel: C,
        f: F,
    ) -> JoinHandle<ScanOutcome<R>>
    where
        F: FnOnce(&mut ScanSlot) -> R + Send + 'static,
        R: Send + 'static,
        C: Future<Output = ()> + Send + 'static,
    {
        let admit = self.admit.clone();
        let io_permits = self.io_permits.clone();
        tokio::spawn(async move {
            let waited = Instant::now();
            let ScanTicket {
                suspended,
                chunk_budget,
            } = ticket;
            let admitted = admit.readmit(suspended, cancel).await;
            run_admitted(admitted, waited, admit, io_permits, chunk_budget, f).await
        })
    }
}

/// The admitted half of [`SchedPool::submit_scan`] and
/// [`SchedPool::resubmit_scan`]: take a bulk-I/O permit, then run `f` on a
/// carrier thread holding both.
async fn run_admitted<F, R>(
    admitted: Admitted,
    waited: Instant,
    admit: Arc<fair_admit::FairAdmit>,
    io_permits: io_permits::IoPermits,
    chunk_budget: u32,
    f: F,
) -> ScanOutcome<R>
where
    F: FnOnce(&mut ScanSlot) -> R + Send + 'static,
    R: Send + 'static,
{
    let id = match admitted {
        Admitted::Slot(id) => id,
        Admitted::Overloaded => return ScanOutcome::Overloaded,
        Admitted::Cancelled => return ScanOutcome::Cancelled,
    };
    record_admission(waited.elapsed());
    // B2 T2.1: hold a bulk-I/O permit while producing (the Lane::Bulk
    // reservation). Acquired as an async task after the CPU slot; only slot
    // holders ever hold one, so at the default 1:1 sizing it is immediate.
    let io_permit = io_permits.acquire().await;
    // Every producer that runs holds a slot, so at most `capacity` carrier
    // threads are busy at once, and none of them ever waits: a slot holder
    // waiting for a carrier thread always gets one as a running producer
    // finishes.
    let out = scan_carrier()
        .spawn_blocking(move || {
            let _busy = CarrierBusy::enter();
            let mut slot = ScanSlot {
                admit,
                id,
                chunk_budget: chunk_budget.max(1),
                since_yield: 0,
                yields: 0,
                last_tick: Instant::now(),
                window_micros: 0,
                _io_permit: Some(io_permit),
                ticket: None,
                finished: false,
            };
            let out = f(&mut slot);
            slot.finish();
            out
        })
        .await
        .expect("scheduler blocking task must not be cancelled");
    ScanOutcome::Ran(out)
}

/// Carrier threads [`scan_carrier`] may run at once: the host's cores. Only
/// producers holding a pool slot run, and the pool never has more slots than
/// cores, so this bounds the threads without limiting the scans.
fn scan_carrier_capacity() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

static SCAN_CARRIER_BUSY: AtomicU64 = AtomicU64::new(0);

/// Carrier threads currently running a scan producer. Never above
/// [`scan_carrier_threads_capacity`].
pub fn scan_carrier_threads_busy() -> u64 {
    SCAN_CARRIER_BUSY.load(Ordering::Relaxed)
}

/// The most carrier threads scan producers can occupy.
pub fn scan_carrier_threads_capacity() -> usize {
    scan_carrier_capacity()
}

/// Marks a carrier thread busy for the life of one producer run.
struct CarrierBusy;

impl CarrierBusy {
    fn enter() -> Self {
        SCAN_CARRIER_BUSY.fetch_add(1, Ordering::Relaxed);
        CarrierBusy
    }
}

impl Drop for CarrierBusy {
    fn drop(&mut self) {
        SCAN_CARRIER_BUSY.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The threads admitted scan producers run on: a runtime owned by the
/// scheduler, used only for its blocking pool, bounded at
/// [`scan_carrier_capacity`].
///
/// Not the caller's `spawn_blocking` pool, because a scan's consumer often
/// needs a thread from that pool to make progress — the PG executor pulls rows
/// with `blocking_recv` on `spawn_blocking` from the PG listener's bounded
/// pool. Sharing it made a cycle: with every thread held by an executor
/// waiting for rows, the producer that would supply them could not get a
/// thread to resume on, and the pool stayed full for good (four executors on a
/// 4-thread pool beside eight slow PG readers).
fn scan_carrier() -> &'static tokio::runtime::Runtime {
    static CARRIER: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    CARRIER.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(scan_carrier_capacity())
            .thread_name("ferrosa-scan")
            .enable_time()
            .build()
            // Fail loud at the first scan: without carrier threads no scan
            // can run at all, and there is no meaningful fallback.
            .expect("ferrosa-sched: building the scan carrier runtime")
    })
}

/// Outcome of a [`SchedPool::submit_scan`] producer.
#[derive(Debug, PartialEq, Eq)]
pub enum ScanOutcome<R> {
    /// The producer was admitted and ran to completion; carries its return value.
    Ran(R),
    /// Admission was shed under overload — the producer never ran. The caller
    /// MUST surface this (fail-loud), e.g. as a read error, never a silent empty
    /// result.
    Overloaded,
    /// The `cancel` signal fired before admission — the consumer went away, so
    /// the producer never ran and never held a slot.
    Cancelled,
}

/// What [`ScanSlot::tick`] tells the producer.
#[must_use = "a producer told to yield has already lost its slot and must return"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tick {
    /// Keep producing.
    Continue,
    /// A more deserving scan waits: the slot is already given up. Return now
    /// and continue in a run admitted with the slot's [`ScanTicket`].
    Yield,
}

/// A scan that stopped mid-range — paused for its consumer or yielded its
/// slot — and its place in the fair queue. Pass it to
/// [`SchedPool::resubmit_scan`] to continue.
#[derive(Debug)]
pub struct ScanTicket {
    suspended: fair_admit::Suspended,
    chunk_budget: u32,
}

/// The fair-share handle a [`SchedPool::submit_scan`] producer holds for one
/// run. Call [`tick`](Self::tick) after each produced chunk.
///
/// No method on it blocks. The run ends when the producer returns; a producer
/// that must stop before its range is done (consumer behind, or told to
/// yield) gives the slot up and returns, and its [`ScanTicket`]
/// ([`take_ticket`](Self::take_ticket)) admits the next run.
pub struct ScanSlot {
    admit: Arc<fair_admit::FairAdmit>,
    id: u64,
    chunk_budget: u32,
    since_yield: u32,
    yields: u64,
    /// When the current chunk's work started (the last `tick` return). Used to
    /// measure per-chunk work time for the FM-2 yield-point-gap tripwire and to
    /// accumulate [`window_micros`](Self::window_micros).
    last_tick: Instant,
    /// Elapsed microseconds accumulated over the current budget window — the
    /// scan's *service time* (B2 T2.2 / DD-1). Elapsed spans both CPU compute
    /// and I/O wait (a chunk blocked on S3 still accrues wall-time here), so
    /// charging `vruntime` by this throttles an I/O-bound scan to its fair share
    /// even when it burns little CPU. Reset at each budget boundary.
    window_micros: u64,
    /// The bulk-I/O permit (B2 T2.1) this run holds while it has its slot,
    /// bounding concurrent bulk I/O on the `Lane::Bulk` reservation. Dropped
    /// with the slot (release, yield, finish, panic), so only slot holders hold
    /// permits.
    _io_permit: Option<io_permits::IoPermit>,
    /// Set when the slot was given up mid-range; admits the next run.
    ticket: Option<ScanTicket>,
    finished: bool,
}

impl ScanSlot {
    /// Account one produced chunk. Records the chunk's work time for the FM-2
    /// tripwire, and every `chunk_budget` calls charges the scan's `vruntime`
    /// (weighted by class). When a waiting scan is now more deserving, the
    /// slot is given up and this returns [`Tick::Yield`]: the producer must
    /// return, and continues in a run admitted with
    /// [`take_ticket`](Self::take_ticket). Never blocks.
    ///
    /// # Panics
    ///
    /// If the slot was already given up: work done on it would be work the
    /// scheduler does not know about.
    pub fn tick(&mut self) -> Tick {
        assert!(!self.finished, "ScanSlot used after it was released");
        let elapsed = self.last_tick.elapsed();
        record_chunk(elapsed);
        // Accumulate the chunk's wall-time (CPU + any I/O wait) as service.
        self.window_micros = self
            .window_micros
            .saturating_add(elapsed.as_micros() as u64);
        self.last_tick = Instant::now();
        self.since_yield += 1;
        if self.since_yield < self.chunk_budget {
            return Tick::Continue;
        }
        self.since_yield = 0;
        self.yields += 1;
        // B2 T2.2 / DD-1: charge `vruntime` by the window's measured elapsed
        // time (CPU compute + I/O wait), weighted by class. Floor at 1 so a
        // sub-microsecond window still advances the clock.
        let service = self.window_micros.max(1);
        self.window_micros = 0;
        match self.admit.charge(self.id, service) {
            None => Tick::Continue,
            Some(suspended) => {
                self.give_up(suspended);
                Tick::Yield
            }
        }
    }

    /// Give the slot (and I/O permit) up because the consumer is not reading.
    /// The producer must return; [`take_ticket`](Self::take_ticket) admits the
    /// run that continues once the consumer has made room. Counted in
    /// [`scan_releases_total`].
    ///
    /// # Panics
    ///
    /// If the slot was already given up.
    pub fn release(&mut self) {
        assert!(!self.finished, "ScanSlot used after it was released");
        let suspended = self.admit.suspend(self.id);
        // Holds by construction: the slot is granted for the whole run, and
        // `tick` marks it finished when it gives it up.
        debug_assert!(suspended.is_some(), "a live ScanSlot holds its slot");
        record_scan_release();
        match suspended {
            Some(suspended) => self.give_up(suspended),
            None => {
                self._io_permit = None;
                self.finished = true;
            }
        }
    }

    /// The ticket for the next run, once the slot was given up mid-range by
    /// [`release`](Self::release) or a [`Tick::Yield`].
    pub fn take_ticket(&mut self) -> Option<ScanTicket> {
        self.ticket.take()
    }

    fn give_up(&mut self, suspended: fair_admit::Suspended) {
        self._io_permit = None;
        self.finished = true;
        self.ticket = Some(ScanTicket {
            suspended,
            chunk_budget: self.chunk_budget,
        });
    }

    /// How many times this run reached a budget boundary.
    pub fn yields(&self) -> u64 {
        self.yields
    }

    fn finish(&mut self) {
        if !self.finished {
            self.finished = true;
            self.admit.finish(self.id);
        }
    }
}

impl Drop for ScanSlot {
    /// RAII backstop: release the slot even if the producer panicked.
    fn drop(&mut self) {
        self.finish();
    }
}

// ---------------------------------------------------------------------------
// Process-global pool
// ---------------------------------------------------------------------------

/// Default cores reserved for consensus when the global pool is not explicitly
/// initialized. Overridable at [`init_global_pool`] time.
pub const DEFAULT_RESERVED_CORES: usize = 1;

/// Partitions (or fragments) a scan produces per pool-slot turn before it
/// cooperatively yields via [`ScanSlot::tick`] (B1 T1.2). Large enough to
/// amortize the re-acquire, small enough that a concurrent scan waits at most
/// this many chunks for its turn.
pub const DEFAULT_SCAN_CHUNK_BUDGET: u32 = 64;

/// Waiters allowed per admission slot before [`SchedPool`] sheds further scans
/// as [`ScanOutcome::Overloaded`]. Bounds the queue so a flood of scans cannot
/// grow it without limit; generous enough that only genuine overload trips it.
pub const DEFAULT_MAX_WAITERS_PER_SLOT: usize = 256;

/// The tenant group every pool scan is charged to today (B3). Until per-query
/// `TenantContext` is threaded through the read path, all interactive scan work
/// shares one group, so the hierarchical [`GroupRunQueue`] is behavior-preserving;
/// folded background work (compaction/repair/index) joins as its *own* group,
/// which is what makes cross-group fair-share arbitration live.
pub const DEFAULT_TENANT_GROUP: GroupId = 0;

static GLOBAL_POOL: std::sync::OnceLock<SchedPool> = std::sync::OnceLock::new();

/// Initialize the process-global [`SchedPool`] with an explicit reservation.
///
/// Call once at boot BEFORE any scan runs. Idempotent via `OnceLock`: the first
/// caller wins, so a later [`global_pool`] fallback never overrides the boot
/// reservation. Returns the installed pool.
pub fn init_global_pool(reservation: Reservation) -> &'static SchedPool {
    GLOBAL_POOL.get_or_init(|| SchedPool::new(reservation))
}

/// The process-global bounded pool. If boot never called [`init_global_pool`],
/// lazily initializes from detected parallelism reserving [`DEFAULT_RESERVED_CORES`]
/// — so the pool is always bounded, never the unbounded default.
pub fn global_pool() -> &'static SchedPool {
    GLOBAL_POOL.get_or_init(|| {
        SchedPool::new(Reservation::from_available_parallelism(
            DEFAULT_RESERVED_CORES,
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn available_is_cores_minus_reserved_floored_at_one() {
        assert_eq!(Reservation::new(8, 2).available(), 6);
        assert_eq!(Reservation::new(4, 4).available(), 1, "must floor at 1");
        assert_eq!(
            Reservation::new(1, 8).available(),
            1,
            "reserved > cores floors at 1"
        );
        assert_eq!(Reservation::new(2, 0).available(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn pool_admits_at_most_capacity_concurrently() {
        // 4 cores, reserve 2 for consensus => capacity 2.
        let pool = SchedPool::new(Reservation::new(4, 2));
        assert_eq!(pool.capacity(), 2);

        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let active = active.clone();
            let max_seen = max_seen.clone();
            let h = pool
                .submit(move || {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_seen.fetch_max(now, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(15));
                    active.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
            handles.push(h);
        }
        for h in handles {
            assert!(
                h.await.expect("blocking task joined").is_some(),
                "closure ran (not shed under overload)"
            );
        }

        assert!(
            max_seen.load(Ordering::SeqCst) <= pool.capacity(),
            "pool admitted {} concurrently, exceeding capacity {}",
            max_seen.load(Ordering::SeqCst),
            pool.capacity()
        );
        assert_eq!(
            pool.available_permits(),
            pool.capacity(),
            "every admission slot must be returned after the tasks finish"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panicking_closure_returns_its_slot() {
        // Capacity 1: if a panic leaked the slot, the second submit would block
        // forever and the test would time out.
        let pool = SchedPool::new(Reservation::new(1, 0));
        assert_eq!(pool.capacity(), 1);

        let h = pool.submit(|| panic!("scan producer blew up")).await;
        assert!(h.await.is_err(), "panicking task must surface a JoinError");

        // The slot must be back; a fresh submit must complete promptly.
        let ok = tokio::time::timeout(Duration::from_secs(5), async {
            pool.submit(|| 42u32)
                .await
                .await
                .expect("second task joined")
                .expect("admitted (not shed under overload)")
        })
        .await
        .expect("slot leaked after panic — second submit blocked");
        assert_eq!(ok, 42);
        assert_eq!(pool.available_permits(), 1);
    }

    /// Unwrap a scan that ran, or fail loudly if it was shed/cancelled.
    fn ran<R: std::fmt::Debug>(outcome: ScanOutcome<R>) -> R {
        match outcome {
            ScanOutcome::Ran(r) => r,
            other => panic!("expected the scan to run, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tick_charges_every_chunk_budget_and_only_yields_to_a_waiter() {
        let pool = SchedPool::new(Reservation::new(1, 0));
        assert_eq!(pool.capacity(), 1);

        // 10 chunks, budget 3 => charged after ticks 3, 6, 9. Nobody waits, so
        // the scan keeps its slot every time.
        let yields = ran(pool
            .submit_scan(SchedClass::Bulk, 3, std::future::pending::<()>(), |slot| {
                for _ in 0..10 {
                    assert_eq!(slot.tick(), Tick::Continue, "no waiter to yield to");
                }
                slot.yields()
            })
            .await
            .expect("scan task joined"));
        assert_eq!(yields, 3, "expected floor(10/3)=3 budget boundaries");
        assert_eq!(pool.available_permits(), 1, "the slot came back");
        assert_eq!(pool.io_permits().in_flight(), 0, "the I/O permit came back");
    }

    /// A producer whose consumer is behind gives back its slot and its I/O
    /// permit and RETURNS, so its thread is free too; its ticket admits the
    /// next run, which re-competes for the slot like any other scan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_released_scan_returns_its_slot_permit_and_thread() {
        let pool = SchedPool::new(Reservation::new(1, 0));
        let releases = scan_releases_total();
        let ticket = ran(pool
            .submit_scan(SchedClass::Bulk, 64, std::future::pending::<()>(), |slot| {
                assert_eq!(slot.tick(), Tick::Continue);
                slot.release();
                slot.take_ticket()
            })
            .await
            .expect("scan joined"))
        .expect("a release leaves a ticket");
        // Process-wide counter, and tests run in parallel: at least ours.
        assert!(scan_releases_total() > releases, "the release is counted");
        assert_eq!(pool.available_permits(), 1, "the slot came back");
        assert_eq!(pool.io_permits().in_flight(), 0, "the I/O permit came back");

        // Another scan holds the only slot; the resumed run must wait for it.
        let (release_other, hold) = std::sync::mpsc::channel::<()>();
        let (other_running, other_started) = tokio::sync::oneshot::channel::<()>();
        let other = pool.submit_scan(
            SchedClass::Bulk,
            64,
            std::future::pending::<()>(),
            move |_slot| {
                other_running.send(()).expect("test waits");
                hold.recv().expect("test releases the slot");
                7u32
            },
        );
        other_started
            .await
            .expect("the other scan was admitted at once");
        let resumed_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let resumed = {
            let resumed_ran = resumed_ran.clone();
            pool.resubmit_scan(ticket, std::future::pending::<()>(), move |_slot| {
                resumed_ran.store(true, Ordering::SeqCst);
                9u32
            })
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !resumed_ran.load(Ordering::SeqCst),
            "a resumed scan ran without a slot while another scan held the only one"
        );
        release_other.send(()).expect("other scan is holding");
        assert_eq!(ran(other.await.expect("other scan joined")), 7);
        assert_eq!(ran(resumed.await.expect("resumed scan joined")), 9);
        assert_eq!(pool.available_permits(), 1, "every slot returned");
        assert_eq!(
            pool.io_permits().in_flight(),
            0,
            "every I/O permit returned"
        );
    }

    #[test]
    #[should_panic(expected = "ScanSlot used after it was released")]
    fn a_released_slot_refuses_further_work() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let pool = SchedPool::new(Reservation::new(1, 0));
        let outcome = rt.block_on(async {
            pool.submit_scan(SchedClass::Bulk, 64, std::future::pending::<()>(), |slot| {
                slot.release();
                let _ = slot.tick();
            })
            .await
        });
        if let Err(join_error) = outcome {
            std::panic::resume_unwind(join_error.into_panic());
        }
    }

    /// An admitted scan runs even when the caller runtime's blocking pool is
    /// full of tasks waiting on that very scan — the PG executor pulling rows
    /// with `blocking_recv` on `spawn_blocking`. One blocking thread, held by a
    /// task that waits for the scan's output: if the producer needed a thread
    /// from that pool it could never start.
    ///
    /// Own runtime with `shutdown_timeout`, so a regression FAILS with a message
    /// instead of hanging on the wedged blocking thread.
    #[test]
    fn a_scan_runs_while_the_callers_blocking_pool_waits_on_it() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let outcome = rt.block_on(async {
            let pool = SchedPool::new(Reservation::new(2, 0));
            let (row_tx, row_rx) = std::sync::mpsc::channel::<u32>();
            // The consumer occupies the runtime's only blocking thread while
            // it waits for the producer's row.
            let consumer = tokio::task::spawn_blocking(move || {
                row_rx.recv_timeout(Duration::from_secs(10)).ok()
            });
            let producer = pool.submit_scan(
                SchedClass::Bulk,
                64,
                std::future::pending::<()>(),
                move |_slot| {
                    let _consumer_listening = row_tx.send(7).is_ok();
                },
            );
            tokio::time::timeout(Duration::from_secs(15), async {
                ran(producer.await.expect("producer joined"));
                consumer.await.expect("consumer joined")
            })
            .await
        });
        rt.shutdown_timeout(Duration::from_secs(1));
        let row = outcome
            .expect("deadlock: the producer waited for a blocking thread held by its own consumer");
        assert_eq!(
            row,
            Some(7),
            "the consumer never got the producer's row: the producer could not get a \
             thread from the pool its consumer was holding"
        );
    }

    /// A scan told to yield gives up its slot AND its I/O permit and returns,
    /// rather than waiting on its thread for the slot to come back. Capacity 1
    /// (one slot, one permit): A ticks with budget 1; once B is queued A gets
    /// `Tick::Yield`, B runs, and A's ticket admits A again after B.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_yielding_scan_returns_and_resumes_after_the_waiter() {
        let pool = SchedPool::new(Reservation::new(1, 0));
        let (a_running_tx, a_running_rx) = tokio::sync::oneshot::channel::<()>();
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));
        let a = {
            let order = order.clone();
            pool.submit_scan(
                SchedClass::Bulk,
                1,
                std::future::pending::<()>(),
                move |slot| {
                    a_running_tx.send(()).expect("the test awaits A");
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while Instant::now() < deadline {
                        std::thread::sleep(Duration::from_millis(1));
                        if slot.tick() == Tick::Yield {
                            order.lock().unwrap().push("a-yielded");
                            return slot.take_ticket();
                        }
                    }
                    None
                },
            )
        };
        a_running_rx.await.expect("scan A started");
        let b = {
            let order = order.clone();
            pool.submit_scan(
                SchedClass::Bulk,
                64,
                std::future::pending::<()>(),
                move |_| {
                    order.lock().unwrap().push("b");
                },
            )
        };
        let ticket = tokio::time::timeout(Duration::from_secs(15), a)
            .await
            .expect("A never returned")
            .expect("scan A joined");
        let ticket = ran(ticket).expect("A was told to yield to the queued B");
        ran(tokio::time::timeout(Duration::from_secs(15), b)
            .await
            .expect("B never ran after A yielded")
            .expect("scan B joined"));
        let resumed = {
            let order = order.clone();
            pool.resubmit_scan(ticket, std::future::pending::<()>(), move |_| {
                order.lock().unwrap().push("a-resumed");
            })
        };
        ran(resumed.await.expect("A resumed"));
        assert_eq!(*order.lock().unwrap(), vec!["a-yielded", "b", "a-resumed"]);
        assert_eq!(pool.available_permits(), 1, "every slot returned");
        assert_eq!(
            pool.io_permits().in_flight(),
            0,
            "every I/O permit returned"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn over_budget_chunk_trips_the_fm2_tripwire() {
        // A chunk whose work exceeds the per-chunk budget must increment the
        // over-budget counter and be reflected in the max-chunk gauge.
        let before = over_budget_chunks_total();
        let pool = SchedPool::new(Reservation::new(1, 0));
        let over_by = Duration::from_micros(DEFAULT_CHUNK_BUDGET_MICROS + 20_000);
        ran(pool
            .submit_scan(
                SchedClass::Bulk,
                1000,
                std::future::pending::<()>(),
                move |slot| {
                    // Simulate a pathologically slow chunk (a huge partition decode).
                    std::thread::sleep(over_by);
                    let _ = slot.tick();
                },
            )
            .await
            .expect("scan task joined"));

        assert!(
            over_budget_chunks_total() > before,
            "an over-budget chunk must increment over_budget_chunks_total"
        );
        assert!(
            sched_max_chunk_micros() >= DEFAULT_CHUNK_BUDGET_MICROS,
            "max_chunk_micros must reflect the slow chunk"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn submit_blocking_bounds_concurrency_and_yields_results() {
        // Sync entry (the scan-producer shape): fire many, only cap run at once.
        let pool = SchedPool::new(Reservation::new(4, 2)); // capacity 2
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for i in 0..16u32 {
            let active = active.clone();
            let max_seen = max_seen.clone();
            // Note: submit_blocking is SYNC (no .await on the call) — the producer
            // call-site shape.
            let h = pool.submit_blocking(move || {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(10));
                active.fetch_sub(1, Ordering::SeqCst);
                i
            });
            handles.push(h);
        }
        let mut sum = 0u32;
        for h in handles {
            sum += h
                .await
                .expect("submit_blocking task joined")
                .expect("admitted (not shed under overload)");
        }
        assert_eq!(
            sum,
            (0..16).sum(),
            "every submitted closure ran and returned"
        );
        assert!(
            max_seen.load(Ordering::SeqCst) <= pool.capacity(),
            "submit_blocking admitted {} concurrently, exceeding capacity {}",
            max_seen.load(Ordering::SeqCst),
            pool.capacity()
        );
    }

    #[test]
    fn global_pool_is_bounded_and_stable() {
        // Lazy init (boot did not run) must still yield a bounded pool, and the
        // same instance every call.
        let p1 = global_pool();
        let p2 = global_pool();
        assert!(
            std::ptr::eq(p1, p2),
            "global pool must be a stable singleton"
        );
        assert!(p1.capacity() >= 1, "global pool is always bounded (>= 1)");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn headroom_reflects_active_and_never_below_reserved() {
        // cores 4, reserved 2 => capacity 2.
        let pool = SchedPool::new(Reservation::new(4, 2));
        assert_eq!(pool.headroom_cores(), 4, "idle headroom == cores");
        assert_eq!(pool.reservation.reserved, 2);

        // Park both slots so `active` == capacity while we sample.
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let rx = rx.clone();
            handles.push(pool.submit_blocking(move || {
                let _ = rx.lock().unwrap().recv(); // park until tx drops
            }));
        }
        for _ in 0..400 {
            if pool.active() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(pool.active(), 2, "both slots admitted");
        assert_eq!(
            pool.headroom_cores(),
            2,
            "headroom == cores - active == reserved when full"
        );
        assert!(
            pool.headroom_cores() >= pool.reservation.reserved,
            "headroom never drops below the consensus reservation"
        );

        drop(tx); // recv() returns Err -> parked tasks finish
        for h in handles {
            let _ = h.await;
        }
        for _ in 0..400 {
            if pool.active() == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(pool.headroom_cores(), 4, "headroom recovers to cores");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn admission_counters_increase() {
        // Process-wide counters are shared across parallel tests, so assert a
        // monotonic DELTA that our submissions must at least account for.
        let before_admitted = tasks_admitted_total();
        let before_wait = admit_wait_micros_total();
        let pool = SchedPool::new(Reservation::new(2, 0));
        let mut handles = Vec::new();
        for _ in 0..5 {
            handles.push(pool.submit_blocking(|| {}));
        }
        for h in handles {
            let _ = h.await.expect("task joined");
        }
        assert!(
            tasks_admitted_total() >= before_admitted + 5,
            "admitted counter must reflect at least our 5 admissions"
        );
        assert!(
            admit_wait_micros_total() >= before_wait,
            "admit-wait micros is monotonic"
        );
    }

    #[test]
    fn render_prometheus_emits_headroom_gauge_and_counters() {
        let text = render_prometheus();
        for metric in [
            "ferrosa_sched_consensus_headroom_cores",
            "ferrosa_sched_pool_capacity",
            "ferrosa_sched_pool_active",
            "ferrosa_sched_tasks_admitted_total",
            "ferrosa_sched_admit_wait_micros_total",
            "ferrosa_sched_admissions_rejected_overload_total",
            "ferrosa_sched_io_permits_acquired_total",
            "ferrosa_sched_io_permits_capacity",
            "ferrosa_sched_io_permits_in_flight",
            "ferrosa_sched_runtime_stall_events_total",
            "ferrosa_sched_runtime_stall_micros_total",
            "ferrosa_sched_runtime_stall_max_micros",
        ] {
            assert!(text.contains(metric), "missing metric {metric}");
        }
        assert!(
            text.contains("# TYPE ferrosa_sched_consensus_headroom_cores gauge"),
            "headroom must be typed as a gauge"
        );
        // The headroom sample line must carry a numeric value.
        let line = text
            .lines()
            .find(|l| l.starts_with("ferrosa_sched_consensus_headroom_cores "))
            .expect("headroom sample line present");
        let value: usize = line
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .expect("headroom value is a number");
        assert!(value >= 1, "global pool headroom is always >= 1 core");
    }
}
