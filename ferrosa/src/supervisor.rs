//! Module: OTP-style supervision for the storage maintenance loop and its flusher.
//! Responsibility: restart a dead or hung background child within a restart
//!   intensity, keep its health observable (`/readyz`, Prometheus, ERROR log
//!   lines on the edges), and escalate when the intensity is exceeded.
//! Correctness: a child is never left dead while the node reports ready; every
//!   failure is counted, and past `max_restarts` failures within `period` the
//!   process aborts after syncing the commit log, so no acknowledged write is
//!   lost and the process supervisor restarts the node from the commit log.
//!   No locks: health is atomics plus `ArcSwapOption`, and each supervisor's
//!   restart window is owned by the one task that drives it.
//! Last revised: 2026-10-03
//! Last changed: New module (t_7681b32b). On 2026-10-02 node2's flush thread
//!   panicked, the maintenance loop logged it once, and the node then flushed,
//!   compacted and persisted nothing while `/readyz` answered 200.
//!
//! ## Children
//!
//! | Child              | Failure kinds counted toward intensity |
//! |--------------------|----------------------------------------|
//! | `storage_flush`    | panic, stall (one per stall deadline)  |
//! | `maintenance_loop` | panic, exit (the loop must never end)  |
//! | `commitlog_sync`   | panic, stall (one per stall deadline)  |
//!
//! `commitlog_sync` is the commit log's background fsync thread (P0-6,
//! t_88479cda). The write path does not depend on this supervisor for
//! durability: the commit log itself refuses writes while its sync thread is
//! dead, failing or stalled (see `ferrosa_storage::commitlog` `sync`). The
//! [`CommitLogSyncSupervisor`] restarts a dead thread, reports the impaired
//! state, and escalates past the intensity.
//!
//! A flush that RETURNS an error marks `storage_flush` impaired until a flush
//! succeeds, but is not counted toward the intensity: the flusher is alive and
//! reporting, and the error paths (disk pressure, refused publication) have
//! their own handling. Only a flusher that died or stopped answering escalates.
//!
//! ## Escalation: crash with context, not "not ready"
//!
//! Exceeding the intensity aborts the process (`std::process::abort`, SIGABRT)
//! after a bounded commit-log sync and a FATAL line on stderr. Not-ready was
//! rejected because nothing gates CQL writes on storage health: a node that
//! only answered 503 would keep acknowledging writes into memtables that can
//! never flush, growing memory and commit-log retention without bound. That is
//! the "up without a flusher" state this module exists to end. The consensus
//! supervisor in `runtime.rs` stays alive instead because a fail-closed CQL
//! gate exists for consensus; none exists for storage.
//!
//! Crashing loses nothing acknowledged: the commit log is the durability
//! boundary and is synced before the abort, and restart replay rebuilds the
//! memtables. Abort rather than `exit(1)`: launchd's `KeepAlive { Crashed }`
//! restarts a crash, not a clean non-zero exit. A deterministic flush panic
//! will crash again after replay; that crash loop is loud by design.
//!
//! ## The commit-log sync supervisor does NOT abort
//!
//! The commit log refuses writes on its own while its sync thread is stalled
//! (`SyncHealthSnapshot::impaired()`: dead, failing, or past the stall
//! deadline), so a stalled node is already acknowledging nothing — the
//! "up without a flusher" state the abort exists to prevent cannot arise here.
//! Aborting such a node only replays the commit log to rejoin and then aborts
//! again on the next stall, so [`CommitLogSyncSupervisor`] counts and logs every
//! deadline and keeps refusing writes instead. It stays loud (one ERROR per
//! deadline on `/readyz`, the metrics and the log) and recovers on its own the
//! moment an fsync completes. A dead or wedged *thread* is still restarted, and
//! `storage_flush` still aborts, because nothing gates CQL writes on flush health.
//!
//! Last revised: 2026-10-06
//! Last changed: the commit-log sync supervisor no longer aborts on a stall;
//!   it counts and logs every deadline, keeps refusing writes, and recovers.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures::FutureExt;
use tokio::sync::oneshot;

/// How long escalation waits for the commit-log sync before aborting anyway.
/// A wedged storage engine must not stop the escalation itself.
const COMMIT_LOG_SYNC_DEADLINE: Duration = Duration::from_secs(10);

/// Default stall deadline for one flush attempt.
pub const DEFAULT_FLUSH_STALL_DEADLINE: Duration = Duration::from_secs(300);

/// A supervised background child.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Child {
    /// The flush attempts the maintenance loop runs on dedicated threads.
    StorageFlush,
    /// The maintenance loop: flush ticks, compaction polling, commit-log GC,
    /// schema persistence and S3 sync.
    MaintenanceLoop,
    /// The commit log's background fsync thread.
    CommitLogSync,
}

impl Child {
    const ALL: [Child; 3] = [
        Child::StorageFlush,
        Child::MaintenanceLoop,
        Child::CommitLogSync,
    ];

    /// Stable label for metrics, logs and the `/readyz` body.
    pub fn label(self) -> &'static str {
        match self {
            Child::StorageFlush => "storage_flush",
            Child::MaintenanceLoop => "maintenance_loop",
            Child::CommitLogSync => "commitlog_sync",
        }
    }

    fn index(self) -> usize {
        match self {
            Child::StorageFlush => 0,
            Child::MaintenanceLoop => 1,
            Child::CommitLogSync => 2,
        }
    }
}

/// Why a child failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    Panic,
    /// An attempt outlived its stall deadline.
    Stall,
    /// An attempt returned an error.
    Error,
    /// A child that must run forever returned.
    Exit,
}

impl FailureKind {
    const ALL: [FailureKind; 4] = [
        FailureKind::Panic,
        FailureKind::Stall,
        FailureKind::Error,
        FailureKind::Exit,
    ];

    fn label(self) -> &'static str {
        match self {
            FailureKind::Panic => "panic",
            FailureKind::Stall => "stall",
            FailureKind::Error => "error",
            FailureKind::Exit => "exit",
        }
    }

    fn index(self) -> usize {
        match self {
            FailureKind::Panic => 0,
            FailureKind::Stall => 1,
            FailureKind::Error => 2,
            FailureKind::Exit => 3,
        }
    }
}

/// OTP restart intensity: more than `max_restarts` failures within `period`
/// escalates.
#[derive(Clone, Copy, Debug)]
pub struct RestartIntensity {
    pub max_restarts: u32,
    pub period: Duration,
}

impl RestartIntensity {
    /// 3 failures per hour. A deterministic flush panic on the 30 s tick
    /// escalates in about 2 minutes; a flush hung for good, recorded once per
    /// 5-minute stall deadline, escalates in about 20.
    pub const DEFAULT: Self = Self {
        max_restarts: 3,
        period: Duration::from_secs(3600),
    };

    /// Read `FERROSA_SUPERVISOR_MAX_RESTARTS` and
    /// `FERROSA_SUPERVISOR_PERIOD_SECS`; an unparseable value is reported and
    /// replaced by the default.
    pub fn from_env() -> Self {
        Self {
            max_restarts: env_or(
                "FERROSA_SUPERVISOR_MAX_RESTARTS",
                Self::DEFAULT.max_restarts,
            ),
            period: Duration::from_secs(env_or(
                "FERROSA_SUPERVISOR_PERIOD_SECS",
                Self::DEFAULT.period.as_secs(),
            )),
        }
    }
}

/// Parse `key` from the environment, logging and falling back on bad input.
pub fn env_or<T: std::str::FromStr + std::fmt::Display + Copy>(key: &str, default: T) -> T {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            tracing::error!(key, value = %raw, %default, "unparseable supervisor setting; using the default");
            default
        }),
    }
}

/// The most recent `max_restarts + 1` failure instants, in a ring of atomics.
///
/// Cloning shares the ring, which is how a supervisor that is rebuilt keeps
/// its history: the flush supervisor is recreated with every maintenance-loop
/// incarnation, and before this was shared a loop restart reset the flush
/// restart window. Exceeded means all `max_restarts + 1` most recent failures
/// fall inside the period, the same rule as an OTP restart intensity.
#[derive(Clone)]
pub struct IntensityWindow {
    intensity: RestartIntensity,
    ring: Arc<FailureRing>,
}

struct FailureRing {
    base: Instant,
    next: AtomicU64,
    /// Nanoseconds since `base`, plus one; 0 is an empty slot.
    slots: Box<[AtomicU64]>,
}

impl IntensityWindow {
    pub fn new(intensity: RestartIntensity) -> Self {
        let len = intensity.max_restarts as usize + 1;
        Self {
            intensity,
            ring: Arc::new(FailureRing {
                base: Instant::now(),
                next: AtomicU64::new(0),
                slots: (0..len).map(|_| AtomicU64::new(0)).collect(),
            }),
        }
    }

    /// Record a failure at `now` and return how many of the most recent
    /// `max_restarts + 1` fall within the period.
    pub(crate) fn record(&self, now: Instant) -> usize {
        let ring = &self.ring;
        let stamp = |at: Instant| {
            at.saturating_duration_since(ring.base)
                .as_nanos()
                .min(u128::from(u64::MAX - 1)) as u64
                + 1
        };
        let len = ring.slots.len() as u64;
        let slot = (ring.next.fetch_add(1, Ordering::AcqRel) % len) as usize;
        ring.slots[slot].store(stamp(now), Ordering::Release);
        ring.slots
            .iter()
            .map(|slot| slot.load(Ordering::Acquire))
            .filter(|&stamped| stamped != 0)
            .map(|stamped| ring.base + Duration::from_nanos(stamped - 1))
            .filter(|&failed| now.saturating_duration_since(failed) < self.intensity.period)
            .count()
    }

    fn exceeded(&self, in_period: usize) -> bool {
        in_period > self.intensity.max_restarts as usize
    }
}

#[derive(Default)]
struct ChildHealth {
    impaired: AtomicBool,
    escalated: AtomicBool,
    failures: [AtomicU64; 4],
    restarts: AtomicU64,
    last_failure: ArcSwapOption<String>,
}

/// The commit-log fsync stall causes, in counter order. The index of a cause
/// is its position here; `stall_cause_index` is an exhaustive match, so adding
/// a `StallCause` variant is a compile error until it is placed.
const STALL_CAUSE_LABELS: [&str; 4] = [
    "device_slow",
    "no_attempt_issued",
    "attempt_failed",
    "sync_thread_dead",
];

/// Where a `StallCause` lands in the per-cause stall counters. Exhaustive on
/// purpose: a new cause cannot be silently left uncounted.
fn stall_cause_index(cause: ferrosa_storage::commitlog::StallCause) -> usize {
    use ferrosa_storage::commitlog::StallCause;
    match cause {
        StallCause::DeviceSlow => 0,
        StallCause::NoAttemptIssued => 1,
        StallCause::AttemptFailed => 2,
        StallCause::ThreadDead => 3,
    }
}

/// Process-wide health of the supervised children. Read by `/readyz` and
/// `/metrics`; written by the supervisors.
#[derive(Default)]
pub struct SupervisionStatus {
    children: [ChildHealth; 3],
    /// Commit-log fsync stalls recorded per `StallCause`. A stall's cause is
    /// only observable at the moment its first deadline fires — the snapshot
    /// has moved on by the next check — so it is counted here rather than
    /// reconstructed from live state.
    sync_stall_causes: [AtomicU64; 4],
}

impl SupervisionStatus {
    fn child(&self, child: Child) -> &ChildHealth {
        &self.children[child.index()]
    }

    /// Count a failure and mark the child impaired. Returns `true` on the
    /// healthy -> impaired edge.
    pub(crate) fn record_failure(&self, child: Child, kind: FailureKind, detail: &str) -> bool {
        let health = self.child(child);
        health.failures[kind.index()].fetch_add(1, Ordering::Relaxed);
        health
            .last_failure
            .store(Some(Arc::new(format!("{}: {detail}", kind.label()))));
        !health.impaired.swap(true, Ordering::AcqRel)
    }

    /// Count a commit-log fsync stall by cause, at the moment its first
    /// deadline fires and the cause is still observable.
    pub(crate) fn record_stall_cause(&self, cause: ferrosa_storage::commitlog::StallCause) {
        self.sync_stall_causes[stall_cause_index(cause)].fetch_add(1, Ordering::Relaxed);
    }

    /// Commit-log fsync stalls since startup for `cause`.
    pub fn sync_stall_causes(&self, cause: ferrosa_storage::commitlog::StallCause) -> u64 {
        self.sync_stall_causes[stall_cause_index(cause)].load(Ordering::Relaxed)
    }

    /// Mark the child healthy. Returns `true` on the impaired -> healthy edge.
    pub(crate) fn record_recovery(&self, child: Child) -> bool {
        self.child(child).impaired.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn record_restart(&self, child: Child) {
        self.child(child).restarts.fetch_add(1, Ordering::Relaxed);
    }

    fn record_escalation(&self, child: Child) {
        let health = self.child(child);
        health.escalated.store(true, Ordering::Release);
        health.impaired.store(true, Ordering::Release);
    }

    fn last_failure(&self, child: Child) -> String {
        self.child(child)
            .last_failure
            .load_full()
            .map(|detail| detail.as_ref().clone())
            .unwrap_or_else(|| "no failure recorded".to_string())
    }

    /// The impaired children as `(label, last failure)`.
    pub fn impaired(&self) -> Vec<(&'static str, String)> {
        Child::ALL
            .into_iter()
            .filter(|child| self.child(*child).impaired.load(Ordering::Acquire))
            .map(|child| (child.label(), self.last_failure(child)))
            .collect()
    }

    pub fn failures(&self, child: Child, kind: FailureKind) -> u64 {
        self.child(child).failures[kind.index()].load(Ordering::Relaxed)
    }

    pub fn restarts(&self, child: Child) -> u64 {
        self.child(child).restarts.load(Ordering::Relaxed)
    }

    /// Prometheus text for every child: `ferrosa_supervised_task_up`,
    /// `_failures_total{kind}`, `_restarts_total` and `_escalated`.
    pub fn render_prometheus(&self, out: &mut String) {
        use std::fmt::Write;
        out.push_str(
            "# HELP ferrosa_supervised_task_up 1 when a supervised background task is healthy, 0 while it is failing, stalled or restarting.\n\
             # TYPE ferrosa_supervised_task_up gauge\n",
        );
        for child in Child::ALL {
            let up = !self.child(child).impaired.load(Ordering::Acquire);
            // Writing into a String cannot fail.
            let _ = writeln!(
                out,
                "ferrosa_supervised_task_up{{task=\"{}\"}} {}",
                child.label(),
                u8::from(up)
            );
        }
        out.push_str(
            "# HELP ferrosa_supervised_task_failures_total Failures of a supervised background task by kind.\n\
             # TYPE ferrosa_supervised_task_failures_total counter\n",
        );
        for child in Child::ALL {
            for kind in FailureKind::ALL {
                let _ = writeln!(
                    out,
                    "ferrosa_supervised_task_failures_total{{task=\"{}\",kind=\"{}\"}} {}",
                    child.label(),
                    kind.label(),
                    self.failures(child, kind)
                );
            }
        }
        out.push_str(
            "# HELP ferrosa_supervised_task_restarts_total Restarts of a supervised background task after a crash or stall.\n\
             # TYPE ferrosa_supervised_task_restarts_total counter\n",
        );
        for child in Child::ALL {
            let _ = writeln!(
                out,
                "ferrosa_supervised_task_restarts_total{{task=\"{}\"}} {}",
                child.label(),
                self.restarts(child)
            );
        }
        out.push_str(
            "# HELP ferrosa_commitlog_sync_stalls_total Commit-log fsync stalls by cause. \
             cause=no_attempt_issued means no fsync was ever issued during the stall — the sync \
             thread did not run, which a restart may help; device_slow means an fsync was in \
             flight the whole time, which it will not.\n\
             # TYPE ferrosa_commitlog_sync_stalls_total counter\n",
        );
        for (i, label) in STALL_CAUSE_LABELS.iter().enumerate() {
            let count = self.sync_stall_causes[i].load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "ferrosa_commitlog_sync_stalls_total{{cause=\"{label}\"}} {count}"
            );
        }
    }
}

/// What escalation reports.
pub struct EscalationReport {
    pub child: Child,
    pub failures_in_period: usize,
    pub intensity: RestartIntensity,
    pub last_failure: String,
}

/// What to do when a child exceeds its restart intensity.
pub enum EscalationPolicy {
    /// Production: sync the commit log (bounded wait), print FATAL, abort.
    AbortProcess {
        engine: Arc<ferrosa_storage::StorageEngine>,
    },
    /// Tests: count escalations and return.
    #[cfg(test)]
    Record(Arc<AtomicU64>),
}

impl EscalationPolicy {
    fn escalate(&self, status: &SupervisionStatus, report: &EscalationReport) {
        status.record_escalation(report.child);
        tracing::error!(
            task = report.child.label(),
            failures_in_period = report.failures_in_period,
            max_restarts = report.intensity.max_restarts,
            period_secs = report.intensity.period.as_secs(),
            last_failure = %report.last_failure,
            "supervised task exceeded its restart intensity; escalating"
        );
        match self {
            EscalationPolicy::AbortProcess { engine } => {
                let synced = sync_commit_log_bounded(engine);
                eprintln!(
                    "FATAL: supervised task exceeded its restart intensity; aborting so the \
process supervisor restarts the node from the commit log. task={} failures_in_period={} \
max_restarts={} period_secs={} commit_log_sync={synced} last_failure={}",
                    report.child.label(),
                    report.failures_in_period,
                    report.intensity.max_restarts,
                    report.intensity.period.as_secs(),
                    report.last_failure
                );
                std::process::abort();
            }
            #[cfg(test)]
            EscalationPolicy::Record(count) => {
                count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

/// Sync the commit log on its own thread and wait at most
/// [`COMMIT_LOG_SYNC_DEADLINE`]. Returns `ok` or why it did not complete.
fn sync_commit_log_bounded(engine: &Arc<ferrosa_storage::StorageEngine>) -> String {
    let engine = Arc::clone(engine);
    let (tx, rx) = std::sync::mpsc::channel();
    let spawned = std::thread::Builder::new()
        .name("escalation-cl-sync".into())
        .spawn(move || {
            // The receiver is gone only when the deadline passed; the
            // escalation already reported that.
            let _ = tx.send(engine.force_commit_log_sync());
        });
    if let Err(e) = spawned {
        return format!("not-attempted(spawn failed: {e})");
    }
    match rx.recv_timeout(COMMIT_LOG_SYNC_DEADLINE) {
        Ok(Ok(())) => "ok".to_string(),
        Ok(Err(e)) => format!("failed({e})"),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            format!("timed-out(after {}s)", COMMIT_LOG_SYNC_DEADLINE.as_secs())
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            "failed(sync thread died)".to_string()
        }
    }
}

/// The text of a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// The result one flush thread reports: the flush's own result, or the panic.
type AttemptResult = Result<ferrosa_common::Result<()>, String>;

/// A flush attempt that outlived its stall deadline and has not reported yet.
struct InFlight {
    thread_name: &'static str,
    started: Instant,
    stalls_recorded: u32,
    rx: oneshot::Receiver<AttemptResult>,
}

/// What one [`FlushSupervisor::run`] call observed.
#[derive(Debug, PartialEq, Eq)]
pub enum FlushRun {
    Flushed,
    /// The flush returned an error.
    Failed,
    Panicked,
    /// The attempt passed its stall deadline; it keeps running detached.
    Stalled,
    /// A stalled attempt is still running, so no new one was started.
    Busy,
    SpawnFailed,
}

/// Runs flush attempts on dedicated threads, one at a time.
///
/// Each attempt is a fresh thread with a `catch_unwind` boundary, so a panic is
/// a crash of that attempt only and the next tick is its restart. The caller
/// waits at most the stall deadline; an attempt past it keeps running detached,
/// is reported as stalled once per elapsed deadline, and blocks new attempts
/// until it reports (a second flush would only queue on the same flush lock).
pub struct FlushSupervisor {
    status: Arc<SupervisionStatus>,
    window: IntensityWindow,
    intensity: RestartIntensity,
    stall_deadline: Duration,
    escalation: Arc<EscalationPolicy>,
    /// Set once escalation ran; only reachable under a policy that returns.
    escalated: bool,
    in_flight: Option<InFlight>,
}

impl FlushSupervisor {
    /// A flush supervisor with its own restart window (tests; the
    /// maintenance loop shares one through [`Self::with_window`]).
    #[cfg(test)]
    pub fn new(
        status: Arc<SupervisionStatus>,
        intensity: RestartIntensity,
        stall_deadline: Duration,
        escalation: Arc<EscalationPolicy>,
    ) -> Self {
        Self::with_window(
            status,
            IntensityWindow::new(intensity),
            stall_deadline,
            escalation,
        )
    }

    /// A flush supervisor counting failures in a window that outlives this
    /// supervisor (one per maintenance-loop incarnation shares it).
    pub(crate) fn with_window(
        status: Arc<SupervisionStatus>,
        window: IntensityWindow,
        stall_deadline: Duration,
        escalation: Arc<EscalationPolicy>,
    ) -> Self {
        assert!(
            !stall_deadline.is_zero(),
            "a zero stall deadline would report every flush as stalled"
        );
        let intensity = window.intensity;
        Self {
            status,
            window,
            intensity,
            stall_deadline,
            escalation,
            escalated: false,
            in_flight: None,
        }
    }

    /// Whether an earlier attempt is still running.
    pub fn is_busy(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Run `work` as a supervised flush attempt on a thread named
    /// `thread_name`, unless an earlier attempt is still running.
    pub async fn run<W>(&mut self, thread_name: &'static str, work: W) -> FlushRun
    where
        W: FnOnce() -> ferrosa_common::Result<()> + Send + 'static,
    {
        if self.poll_in_flight() {
            return FlushRun::Busy;
        }
        let (tx, mut rx) = oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name(thread_name.into())
            .spawn(move || {
                let result = std::panic::catch_unwind(AssertUnwindSafe(work))
                    .map_err(|payload| panic_message(payload.as_ref()));
                if tx.send(result).is_err() {
                    tracing::error!(
                        thread = thread_name,
                        "flush attempt finished after its supervisor stopped waiting; \
                         its result was not observed"
                    );
                }
            });
        if let Err(e) = spawned {
            self.record_error(format!("could not spawn {thread_name}: {e}"));
            return FlushRun::SpawnFailed;
        }
        let started = Instant::now();
        match tokio::time::timeout(self.stall_deadline, &mut rx).await {
            Ok(received) => self.settle(thread_name, received.ok()),
            Err(_elapsed) => {
                self.in_flight = Some(InFlight {
                    thread_name,
                    started,
                    stalls_recorded: 0,
                    rx,
                });
                self.record_stalls();
                FlushRun::Stalled
            }
        }
    }

    /// Settle a finished in-flight attempt. Returns `true` while one is still
    /// running (after recording any newly elapsed stall deadlines).
    fn poll_in_flight(&mut self) -> bool {
        let Some(in_flight) = self.in_flight.as_mut() else {
            return false;
        };
        let thread_name = in_flight.thread_name;
        match in_flight.rx.try_recv() {
            Ok(result) => {
                self.in_flight = None;
                self.settle(thread_name, Some(result));
                false
            }
            Err(oneshot::error::TryRecvError::Closed) => {
                self.in_flight = None;
                self.settle(thread_name, None);
                false
            }
            Err(oneshot::error::TryRecvError::Empty) => {
                self.record_stalls();
                true
            }
        }
    }

    /// Record one stall per stall deadline the in-flight attempt has outlived.
    fn record_stalls(&mut self) {
        let Some(in_flight) = self.in_flight.as_mut() else {
            return;
        };
        let elapsed = in_flight.started.elapsed();
        let due =
            (elapsed.as_nanos() / self.stall_deadline.as_nanos()).min(u32::MAX as u128) as u32;
        let thread_name = in_flight.thread_name;
        let newly_due = due.saturating_sub(in_flight.stalls_recorded);
        in_flight.stalls_recorded = due.max(in_flight.stalls_recorded);
        for _ in 0..newly_due {
            self.record_crash(
                FailureKind::Stall,
                format!(
                    "{thread_name} stalled: running for {}s, past the {}s stall deadline",
                    elapsed.as_secs(),
                    self.stall_deadline.as_secs_f64()
                ),
            );
        }
    }

    fn settle(&mut self, thread_name: &'static str, result: Option<AttemptResult>) -> FlushRun {
        match result {
            Some(Ok(Ok(()))) => {
                if self.status.record_recovery(Child::StorageFlush) {
                    tracing::warn!(
                        task = Child::StorageFlush.label(),
                        "storage flush recovered: a flush completed after failures"
                    );
                }
                FlushRun::Flushed
            }
            Some(Ok(Err(e))) => {
                self.record_error(format!("{thread_name} failed: {e}"));
                FlushRun::Failed
            }
            Some(Err(panic)) => {
                self.record_crash(
                    FailureKind::Panic,
                    format!("{thread_name} panicked: {panic}"),
                );
                FlushRun::Panicked
            }
            None => {
                self.record_crash(
                    FailureKind::Panic,
                    format!("{thread_name} exited without reporting a result"),
                );
                FlushRun::Panicked
            }
        }
    }

    /// A returned error: impaired until a flush succeeds, ERROR on the edge.
    fn record_error(&self, detail: String) {
        if self
            .status
            .record_failure(Child::StorageFlush, FailureKind::Error, &detail)
        {
            tracing::error!(
                task = Child::StorageFlush.label(),
                %detail,
                "storage flush failing; the node reports not ready until a flush succeeds"
            );
        } else {
            tracing::debug!(task = Child::StorageFlush.label(), %detail, "storage flush still failing");
        }
    }

    /// A panic or stall: counted toward the intensity, escalated past it.
    fn record_crash(&mut self, kind: FailureKind, detail: String) {
        self.status
            .record_failure(Child::StorageFlush, kind, &detail);
        let in_period = self.window.record(Instant::now());
        if self.escalated {
            return;
        }
        if self.window.exceeded(in_period) {
            self.escalated = true;
            self.escalation.escalate(
                &self.status,
                &EscalationReport {
                    child: Child::StorageFlush,
                    failures_in_period: in_period,
                    intensity: self.intensity,
                    last_failure: detail,
                },
            );
            return;
        }
        self.status.record_restart(Child::StorageFlush);
        tracing::error!(
            task = Child::StorageFlush.label(),
            kind = kind.label(),
            failures_in_period = in_period,
            max_restarts = self.intensity.max_restarts,
            %detail,
            "storage flush crashed; the next flush tick restarts it, and the node reports \
             not ready until a flush succeeds"
        );
    }
}

/// How often the commit-log sync supervisor samples the sync thread's health.
pub const COMMIT_LOG_SYNC_POLL: Duration = Duration::from_millis(100);

/// What the commit-log sync supervisor watches and restarts. A trait so the
/// tests can drive the supervisor without a disk.
pub trait CommitLogSyncTarget: Send + Sync {
    fn sync_health(&self) -> ferrosa_storage::commitlog::SyncHealthSnapshot;
    fn restart_sync(&self) -> ferrosa_common::Result<bool>;
}

impl CommitLogSyncTarget for ferrosa_storage::StorageEngine {
    fn sync_health(&self) -> ferrosa_storage::commitlog::SyncHealthSnapshot {
        self.commit_log_sync_health()
    }

    fn restart_sync(&self) -> ferrosa_common::Result<bool> {
        self.restart_commit_log_sync()
    }
}

impl CommitLogSyncTarget for ferrosa_storage::commitlog::CommitLog {
    fn sync_health(&self) -> ferrosa_storage::commitlog::SyncHealthSnapshot {
        ferrosa_storage::commitlog::CommitLog::sync_health(self)
    }

    fn restart_sync(&self) -> ferrosa_common::Result<bool> {
        ferrosa_storage::commitlog::CommitLog::restart_sync(self)
    }
}

/// Supervises the commit log's fsync thread from health samples.
///
/// Each [`check`](Self::check) compares a fresh snapshot with the last one:
/// a new panic is a crash (counted, then the thread is restarted), every
/// elapsed stall deadline is a stall (counted), a failing fsync marks the
/// child impaired without counting (the stall it causes is counted). Past the
/// intensity it escalates instead of restarting. It is healthy again only
/// when the snapshot is: alive, the last fsync succeeded, and no write has
/// waited past the stall deadline.
pub struct CommitLogSyncSupervisor<T: CommitLogSyncTarget + ?Sized> {
    target: Arc<T>,
    status: Arc<SupervisionStatus>,
    window: IntensityWindow,
    intensity: RestartIntensity,
    escalation: Arc<EscalationPolicy>,
    escalated: bool,
    seen_panics: u64,
    seen_sync_failures: u64,
    /// Stall deadlines already recorded for the current stall episode.
    stalls_recorded: u64,
    /// `attempts_completed` at the previous sample. A completion since then
    /// means the sync thread is alive and making progress, so the backlog
    /// behind it is age, not the death this supervisor aborts on.
    seen_sync_completions: u64,
}

impl<T: CommitLogSyncTarget + ?Sized> CommitLogSyncSupervisor<T> {
    pub fn new(
        target: Arc<T>,
        status: Arc<SupervisionStatus>,
        intensity: RestartIntensity,
        escalation: Arc<EscalationPolicy>,
    ) -> Self {
        let baseline = target.sync_health();
        Self {
            target,
            status,
            window: IntensityWindow::new(intensity),
            intensity,
            escalation,
            escalated: false,
            seen_panics: baseline.panics,
            seen_sync_failures: baseline.sync_failures,
            stalls_recorded: 0,
            seen_sync_completions: baseline.attempts_completed,
        }
    }

    /// Sample the sync thread's health once and act on it.
    pub fn check(&mut self) {
        let health = self.target.sync_health();
        let detail = health
            .last_failure
            .clone()
            .unwrap_or_else(|| "no failure recorded".to_string());

        let new_panics = health.panics.saturating_sub(self.seen_panics);
        self.seen_panics = health.panics;
        // Restarted within one poll and died again: each death is counted.
        for _ in 0..new_panics.min(u64::from(self.intensity.max_restarts) + 1) {
            self.record_crash(FailureKind::Panic, detail.clone());
        }
        if health.dead && !self.escalated {
            self.restart(&detail);
        }

        self.record_stalls(&health, &detail);

        if health.sync_failures > self.seen_sync_failures {
            self.seen_sync_failures = health.sync_failures;
            if self
                .status
                .record_failure(Child::CommitLogSync, FailureKind::Error, &detail)
            {
                tracing::error!(
                    task = Child::CommitLogSync.label(),
                    %detail,
                    "commit-log fsync failing; writes are refused and the node reports not ready \
                     until an fsync succeeds"
                );
            }
        }

        // A thread restarted in this check has synced nothing yet; the next
        // sample decides whether it recovered.
        if !health.dead
            && !health.impaired()
            && !self.escalated
            && self.status.record_recovery(Child::CommitLogSync)
        {
            tracing::warn!(
                task = Child::CommitLogSync.label(),
                "commit-log sync recovered; writes are acknowledged again"
            );
        }
    }

    fn restart(&mut self, detail: &str) {
        match self.target.restart_sync() {
            Ok(true) => {
                self.status.record_restart(Child::CommitLogSync);
                tracing::error!(
                    task = Child::CommitLogSync.label(),
                    %detail,
                    "commit-log sync thread died and was restarted; it syncs what the dead \
                     thread left before writes are acknowledged again"
                );
            }
            Ok(false) => {}
            Err(e) => {
                // Writes stay refused (the thread is still dead); the next
                // check retries the restart.
                self.status.record_failure(
                    Child::CommitLogSync,
                    FailureKind::Error,
                    &format!("restart failed: {e}"),
                );
                tracing::error!(
                    task = Child::CommitLogSync.label(),
                    %e,
                    "could not restart the commit-log sync thread; writes stay refused and the \
                     next check retries"
                );
            }
        }
    }

    /// One stall per stall deadline the oldest unsynced write has outlived.
    fn record_stalls(
        &mut self,
        health: &ferrosa_storage::commitlog::SyncHealthSnapshot,
        detail: &str,
    ) {
        let Some(waited) = health.unsynced_for.filter(|_| health.stalled()) else {
            self.stalls_recorded = 0;
            return;
        };
        // The cause is knowable only while this stall is current: the snapshot
        // has moved on (or recovered) by the next check. Record it now, and put
        // it in the log line an operator actually reads — this is the wiring
        // t_ca4e1f81 was left without.
        let Some(cause) = health.stall_cause() else {
            tracing::error!(
                task = Child::CommitLogSync.label(),
                "commit-log sync reports stalled with no stall cause; not counting this stall"
            );
            return;
        };
        // `unsynced_for` is the age of the *oldest unsynced write*, not the
        // duration of one in-flight fsync. On a node under continuous write the
        // backlog never empties, so the episode below never ends even though the
        // thread is completing fsyncs the whole time. An fsync completed since
        // the previous sample is definitive proof of a live thread, so this
        // sample is backlog age rather than the death this aborts on.
        let made_progress = health.attempts_completed > self.seen_sync_completions;
        self.seen_sync_completions = health.attempts_completed;
        let due =
            (waited.as_nanos() / health.stall_deadline.as_nanos()).min(u128::from(u64::MAX)) as u64;
        let first_new = self.stalls_recorded + 1;
        let newly_due = due.saturating_sub(self.stalls_recorded);
        self.stalls_recorded = due.max(self.stalls_recorded);
        let detail = format!(
            "no commit-log fsync for {}ms, past the {}ms stall deadline (cause={cause}, {detail})",
            waited.as_millis(),
            health.stall_deadline.as_millis()
        );
        if made_progress {
            // The thread completed an fsync since the previous sample, so the
            // backlog is age rather than a dead thread. Count the episode once
            // and never escalate: a slow disk is not a crash.
            if first_new == 1
                && self
                    .status
                    .record_failure(Child::CommitLogSync, FailureKind::Stall, &detail)
            {
                self.status.record_stall_cause(cause);
                tracing::error!(
                    task = Child::CommitLogSync.label(),
                    %detail,
                    "commit-log fsync backlog is past the stall deadline while the sync thread \
                     keeps completing fsyncs; writes are refused and the node reports not ready \
                     until the backlog clears"
                );
            }
            return;
        }

        // No fsync completed since the previous sample: the thread is genuinely
        // stuck. Count and log every deadline so the stall stays loud and
        // alertable, but do NOT abort. The commit log refuses writes on its own
        // while stalled (`SyncHealthSnapshot::impaired()`), so the node is
        // acknowledging nothing and has no unflushed work to lose; aborting
        // would only replay the commit log to rejoin and then abort again on the
        // next stall. Staying up keeps the node observable and lets the stall
        // heal on its own when an fsync finally completes.
        for deadline in
            (first_new..).take(newly_due.min(u64::from(self.intensity.max_restarts) + 2) as usize)
        {
            if deadline == 1 {
                if self
                    .status
                    .record_failure(Child::CommitLogSync, FailureKind::Stall, &detail)
                {
                    self.status.record_stall_cause(cause);
                    tracing::error!(
                        task = Child::CommitLogSync.label(),
                        %detail,
                        "commit-log fsync stalled; writes are refused and the node reports not \
                         ready until an fsync completes"
                    );
                }
            } else {
                self.status
                    .record_failure(Child::CommitLogSync, FailureKind::Stall, &detail);
                tracing::error!(
                    task = Child::CommitLogSync.label(),
                    %detail,
                    "commit-log fsync has still not completed; writes stay refused and the node \
                     stays not ready. Not aborting: a stalled commit log refuses writes, so \
                     staying up loses nothing and avoids a replay"
                );
            }
        }
    }

    /// A panic or stall: counted toward the intensity, escalated past it.
    fn record_crash(&mut self, kind: FailureKind, detail: String) {
        self.status
            .record_failure(Child::CommitLogSync, kind, &detail);
        let in_period = self.window.record(Instant::now());
        if self.escalated {
            return;
        }
        if self.window.exceeded(in_period) {
            self.escalated = true;
            self.escalation.escalate(
                &self.status,
                &EscalationReport {
                    child: Child::CommitLogSync,
                    failures_in_period: in_period,
                    intensity: self.intensity,
                    last_failure: detail,
                },
            );
            return;
        }
        tracing::error!(
            task = Child::CommitLogSync.label(),
            kind = kind.label(),
            failures_in_period = in_period,
            max_restarts = self.intensity.max_restarts,
            %detail,
            "commit-log sync failed; writes are refused and the node reports not ready until \
             it recovers"
        );
    }
}

/// Run a [`CommitLogSyncSupervisor`] every `poll` until the process exits.
///
/// Each check runs on the blocking pool: a restart joins the dead thread and
/// waits (bounded by the stall deadline) for the new one to sync its backlog,
/// which must never hold an async worker. A panic inside a check is re-raised
/// here, so the enclosing supervision sees it.
pub async fn run_commit_log_sync_supervisor<T: CommitLogSyncTarget + ?Sized + 'static>(
    mut supervisor: CommitLogSyncSupervisor<T>,
    poll: Duration,
) {
    let mut tick = tokio::time::interval(poll);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        supervisor = match tokio::task::spawn_blocking(move || {
            supervisor.check();
            supervisor
        })
        .await
        {
            Ok(supervisor) => supervisor,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => panic!("commit-log sync supervisor check was cancelled: {e}"),
        };
    }
}

/// Run `start()` forever, restarting it when it panics or returns, and
/// escalate past `intensity`.
///
/// The child is restarted at once; its state is rebuilt by `start`. A restart
/// clears the impaired flag (the child runs again); the failure stays visible
/// in the restart and failure counters and the ERROR line. Under a policy that
/// returns from escalation (tests), this returns after escalating.
pub async fn supervise<F, Fut>(
    child: Child,
    status: Arc<SupervisionStatus>,
    intensity: RestartIntensity,
    escalation: Arc<EscalationPolicy>,
    mut start: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let window = IntensityWindow::new(intensity);
    loop {
        // The child future owns its state; whatever a panic left half-updated
        // is dropped with it, so asserting unwind safety is sound.
        // Calling `start` inside the future puts a panic in `start` itself
        // under the same boundary.
        let exit = AssertUnwindSafe(async { start().await })
            .catch_unwind()
            .await;
        let (kind, detail) = match exit {
            Ok(()) => (
                FailureKind::Exit,
                "returned; it must run until the process exits".to_string(),
            ),
            Err(payload) => (FailureKind::Panic, panic_message(payload.as_ref())),
        };
        status.record_failure(child, kind, &detail);
        let in_period = window.record(Instant::now());
        if window.exceeded(in_period) {
            escalation.escalate(
                &status,
                &EscalationReport {
                    child,
                    failures_in_period: in_period,
                    intensity,
                    last_failure: detail,
                },
            );
            return;
        }
        status.record_restart(child);
        tracing::error!(
            task = child.label(),
            kind = kind.label(),
            failures_in_period = in_period,
            max_restarts = intensity.max_restarts,
            %detail,
            "supervised task died; restarting it"
        );
        status.record_recovery(child);
    }
}

/// The last time a supervised loop made progress, as one atomic.
pub struct Heartbeat {
    base: Instant,
    /// Nanoseconds since `base` of the last beat.
    last: AtomicU64,
}

impl Default for Heartbeat {
    fn default() -> Self {
        Self::new()
    }
}

impl Heartbeat {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            last: AtomicU64::new(0),
        }
    }

    pub fn beat(&self) {
        self.beat_at(Instant::now());
    }

    pub(crate) fn beat_at(&self, now: Instant) {
        let nanos = now
            .saturating_duration_since(self.base)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64;
        self.last.fetch_max(nanos, Ordering::AcqRel);
    }

    /// How long since the last beat, as of `now`.
    pub(crate) fn silent_for(&self, now: Instant) -> Duration {
        let last = self.base + Duration::from_nanos(self.last.load(Ordering::Acquire));
        now.saturating_duration_since(last)
    }
}

/// Default for how long the maintenance loop may go without a beat.
pub fn default_maintenance_stall_deadline(flush_stall_deadline: Duration) -> Duration {
    // One arm may legitimately wait a whole flush stall deadline for the
    // flusher; twice that is a loop that is not coming back on its own.
    flush_stall_deadline.saturating_mul(2)
}

/// Watches the maintenance loop's [`Heartbeat`] from outside the loop.
///
/// The loop beats on every iteration. A hang the loop cannot report itself
/// (an `.await` in `poll_compactions` that never completes, commit-log GC
/// blocked on a lock) stops the beats: each elapsed `deadline` of silence is
/// a `maintenance_loop` stall, counted toward the intensity and escalated
/// past it. A hung future cannot be restarted from outside; escalation
/// (commit-log sync, then abort) is the restart.
pub struct MaintenanceWatchdog {
    heartbeat: Arc<Heartbeat>,
    deadline: Duration,
    status: Arc<SupervisionStatus>,
    window: IntensityWindow,
    intensity: RestartIntensity,
    escalation: Arc<EscalationPolicy>,
    stalls_recorded: u64,
    escalated: bool,
}

impl MaintenanceWatchdog {
    pub fn new(
        heartbeat: Arc<Heartbeat>,
        deadline: Duration,
        status: Arc<SupervisionStatus>,
        intensity: RestartIntensity,
        escalation: Arc<EscalationPolicy>,
    ) -> Self {
        assert!(
            !deadline.is_zero(),
            "a zero deadline would stall every check"
        );
        Self {
            heartbeat,
            deadline,
            status,
            window: IntensityWindow::new(intensity),
            intensity,
            escalation,
            stalls_recorded: 0,
            escalated: false,
        }
    }

    /// How often [`spawn_maintenance_watchdog`]'s thread checks.
    pub fn poll_interval(&self) -> Duration {
        (self.deadline / 10).max(Duration::from_millis(100))
    }

    pub fn check(&mut self, now: Instant) {
        let silent = self.heartbeat.silent_for(now);
        if silent < self.deadline {
            if self.stalls_recorded > 0 {
                self.stalls_recorded = 0;
                if !self.escalated && self.status.record_recovery(Child::MaintenanceLoop) {
                    tracing::warn!(
                        task = Child::MaintenanceLoop.label(),
                        "maintenance loop is making progress again"
                    );
                }
            }
            return;
        }
        let due = (silent.as_nanos() / self.deadline.as_nanos()).min(u128::from(u64::MAX)) as u64;
        let newly_due = due.saturating_sub(self.stalls_recorded);
        self.stalls_recorded = due.max(self.stalls_recorded);
        let detail = format!(
            "no maintenance-loop iteration for {}s, past the {}s watchdog deadline",
            silent.as_secs(),
            self.deadline.as_secs()
        );
        for _ in 0..newly_due.min(u64::from(self.intensity.max_restarts) + 1) {
            self.status
                .record_failure(Child::MaintenanceLoop, FailureKind::Stall, &detail);
            let in_period = self.window.record(now);
            if self.escalated {
                continue;
            }
            if self.window.exceeded(in_period) {
                self.escalated = true;
                self.escalation.escalate(
                    &self.status,
                    &EscalationReport {
                        child: Child::MaintenanceLoop,
                        failures_in_period: in_period,
                        intensity: self.intensity,
                        last_failure: detail.clone(),
                    },
                );
                continue;
            }
            tracing::error!(
                task = Child::MaintenanceLoop.label(),
                failures_in_period = in_period,
                max_restarts = self.intensity.max_restarts,
                %detail,
                "maintenance loop is hung; the node reports not ready"
            );
        }
    }
}

/// Run a [`MaintenanceWatchdog`] on its own OS thread, so a maintenance loop
/// that wedges the runtime it shares cannot also stop its watchdog. A panic in
/// one check is logged and the next check runs.
pub fn spawn_maintenance_watchdog(mut watchdog: MaintenanceWatchdog) -> std::io::Result<()> {
    let poll = watchdog.poll_interval();
    std::thread::Builder::new()
        .name("maintenance-watchdog".into())
        .spawn(move || loop {
            std::thread::park_timeout(poll);
            if let Err(payload) =
                std::panic::catch_unwind(AssertUnwindSafe(|| watchdog.check(Instant::now())))
            {
                tracing::error!(
                    panic = %panic_message(payload.as_ref()),
                    "maintenance watchdog check panicked; the next check runs"
                );
            }
        })
        .map(|_detached| ())
}

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;
