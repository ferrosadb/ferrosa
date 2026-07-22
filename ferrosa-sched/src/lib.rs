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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

pub mod runqueue;
pub mod scheduler;

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
    out
}

/// Scheduling class of a unit of work. Phase 0 does not *act* on the class (the
/// pool admits FIFO); it exists so B1 can attach fair-share weights without
/// re-touching call sites. Consensus work never flows through [`SchedPool`] — it
/// runs on its own reserved runtime — so it has no variant here by design.
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

/// Bounded execution pool for CPU-bound background work.
///
/// [`submit`](SchedPool::submit) runs a blocking closure on tokio's blocking
/// threads but admits at most [`capacity`](SchedPool::capacity) at once, so the
/// reserved consensus cores are never oversubscribed. The admission permit is
/// moved into the blocking task and dropped when it finishes — including on
/// panic — so a slot is never leaked (RAII).
#[derive(Clone)]
pub struct SchedPool {
    slots: Arc<Semaphore>,
    capacity: usize,
    reservation: Reservation,
}

impl SchedPool {
    /// Build a pool admitting `reservation.available()` CPU-bound tasks at once.
    pub fn new(reservation: Reservation) -> Self {
        let capacity = reservation.available();
        Self {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            reservation,
        }
    }

    /// Maximum number of closures admitted concurrently.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Currently free admission slots (`capacity` minus in-flight).
    pub fn available_permits(&self) -> usize {
        self.slots.available_permits()
    }

    /// Background closures currently admitted (running or on a blocking thread).
    pub fn active(&self) -> usize {
        self.capacity.saturating_sub(self.slots.available_permits())
    }

    /// CPU cores currently NOT doing pool work — the consensus headroom
    /// (`SCHED_CONSENSUS_HEADROOM_CORES`). Never drops below `reserved`, so raft
    /// always has a schedulable core. Falls as scans admit, recovers as they end.
    pub fn headroom_cores(&self) -> usize {
        self.reservation.cores.saturating_sub(self.active())
    }

    /// Admit and run `f` on a blocking thread once a slot is free, returning the
    /// join handle. Awaiting this future blocks (asynchronously) until admission;
    /// the returned handle resolves to `f`'s result (or a `JoinError` if `f`
    /// panicked). The admission slot is released when the task ends.
    pub async fn submit<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let waited = Instant::now();
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("scheduler pool semaphore is never closed");
        record_admission(waited.elapsed());
        tokio::task::spawn_blocking(move || {
            // RAII: the permit is released when this closure returns OR unwinds,
            // so a panicking scan producer never leaks its admission slot.
            let _permit = permit;
            f()
        })
    }

    /// Sync entry point for producers that run *inside* a runtime but are not
    /// themselves `async` (the storage scan producers: sync functions that fire a
    /// blocking task and return a stream). Spawns a cheap async admitter that
    /// acquires a slot and only then moves `f` onto a blocking thread — so at most
    /// [`capacity`](Self::capacity) blocking threads ever exist. Excess producers
    /// wait as async tasks, NOT as parked blocking threads (which is what
    /// oversubscribed the cores and starved raft).
    ///
    /// Must be called from within a tokio runtime. The returned handle resolves to
    /// `f`'s result; a panic in `f` surfaces as a `JoinError` (and, being detached
    /// by the callers, is logged by tokio rather than crashing the process).
    pub fn submit_blocking<F, R>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let slots = self.slots.clone();
        tokio::spawn(async move {
            let waited = Instant::now();
            let permit = slots
                .acquire_owned()
                .await
                .expect("scheduler pool semaphore is never closed");
            record_admission(waited.elapsed());
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                f()
            })
            .await
            .expect("scheduler blocking task must not be cancelled")
        })
    }

    /// Sync entry for a *cooperative* scan producer (B1 T1.2). Like
    /// [`submit_blocking`](Self::submit_blocking), but `f` receives a
    /// [`ScanSlot`] and must call [`ScanSlot::tick`] after each produced chunk.
    /// Every `chunk_budget` ticks the slot's pool permit is released and fairly
    /// re-acquired, so a long full-table scan cedes the pool to waiting scans
    /// instead of holding a slot for its whole duration — the fix for one scan
    /// monopolizing the bounded pool (the residual B0 leaves).
    ///
    /// Deadlock-free: the permit is dropped BEFORE the blocking re-acquire, so a
    /// slot is always available to a waiter; and a parked re-acquire consumes no
    /// CPU (it does not oversubscribe the cores B0 protects).
    pub fn submit_scan<F, R>(&self, chunk_budget: u32, f: F) -> JoinHandle<R>
    where
        F: FnOnce(&mut ScanSlot) -> R + Send + 'static,
        R: Send + 'static,
    {
        let slots = self.slots.clone();
        tokio::spawn(async move {
            let waited = Instant::now();
            let permit = slots
                .clone()
                .acquire_owned()
                .await
                .expect("scheduler pool semaphore is never closed");
            record_admission(waited.elapsed());
            let handle = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut slot = ScanSlot {
                    slots,
                    permit: Some(permit),
                    chunk_budget: chunk_budget.max(1),
                    since_yield: 0,
                    yields: 0,
                    handle,
                };
                f(&mut slot)
            })
            .await
            .expect("scheduler blocking task must not be cancelled")
        })
    }
}

/// The cooperative-yield handle a [`SchedPool::submit_scan`] producer holds for
/// its lifetime. Call [`tick`](Self::tick) after each produced chunk; every
/// `chunk_budget` ticks it yields the pool slot (release + fair re-acquire) so
/// concurrent scans interleave instead of one monopolizing the pool.
pub struct ScanSlot {
    slots: Arc<Semaphore>,
    /// The held admission permit. `None` only transiently, mid-yield, while the
    /// slot is released and a replacement is being acquired.
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    chunk_budget: u32,
    since_yield: u32,
    yields: u64,
    handle: tokio::runtime::Handle,
}

impl ScanSlot {
    /// Account one produced chunk. Every `chunk_budget` calls, release the pool
    /// permit so a waiting scan can run, then fairly (FIFO) re-acquire one.
    ///
    /// Runs on a `spawn_blocking` thread (not an async context), so blocking on
    /// the re-acquire via the runtime handle is sound.
    pub fn tick(&mut self) {
        self.since_yield += 1;
        if self.since_yield < self.chunk_budget {
            return;
        }
        self.since_yield = 0;
        self.yields += 1;
        // Release BEFORE re-acquiring: a slot is always free for a waiter.
        self.permit = None;
        let slots = self.slots.clone();
        let waited = Instant::now();
        let permit = self
            .handle
            .block_on(async move { slots.acquire_owned().await })
            .expect("scheduler pool semaphore is never closed");
        record_admission(waited.elapsed());
        self.permit = Some(permit);
    }

    /// How many times this slot has yielded and re-acquired its permit.
    pub fn yields(&self) -> u64 {
        self.yields
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
            h.await.expect("blocking task joined");
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
        })
        .await
        .expect("slot leaked after panic — second submit blocked");
        assert_eq!(ok, 42);
        assert_eq!(pool.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn submit_scan_yields_the_permit_every_chunk_budget_and_restores_it() {
        // Capacity 1: the scan releases + re-acquires the sole permit every
        // `chunk_budget` ticks. With one scan the re-acquire succeeds
        // immediately (it just freed the slot), so this deterministically
        // exercises the real release/re-acquire path without timing races.
        let pool = SchedPool::new(Reservation::new(1, 0));
        assert_eq!(pool.capacity(), 1);

        // 10 chunks, budget 3 => yields after ticks 3, 6, 9 => 3 yields.
        let yields = pool
            .submit_scan(3, |slot| {
                for _ in 0..10 {
                    slot.tick();
                }
                slot.yields()
            })
            .await
            .expect("scan task joined");
        assert_eq!(yields, 3, "expected floor(10/3)=3 slot yields");

        // The permit is fully restored after the scan ends (no leak).
        assert_eq!(pool.available_permits(), 1);

        // A budget larger than the chunk count never yields.
        let yields = pool
            .submit_scan(100, |slot| {
                for _ in 0..5 {
                    slot.tick();
                }
                slot.yields()
            })
            .await
            .expect("scan task joined");
        assert_eq!(yields, 0);
        assert_eq!(pool.available_permits(), 1);
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
            sum += h.await.expect("submit_blocking task joined");
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
            h.await.expect("task joined");
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
