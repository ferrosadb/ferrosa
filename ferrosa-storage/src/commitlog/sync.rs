//! Module: when commit-log segment buffers are fsynced, and when a write may
//!   be acknowledged.
//! Correctness: a write is acknowledged only while the sync machinery keeps up.
//!   A dead sync thread, a failing fsync, or an fsync that has fallen behind
//!   `sync_stall_deadline` makes the write path return
//!   [`Error::CommitLogNotDurable`](ferrosa_common::Error::CommitLogNotDurable)
//!   instead of an acknowledgement. No locks were added: durability state is
//!   atomics ([`SyncHealth`]); the condvar mutexes predate this change.
//! Last revised: 2026-10-03
//! Last changed: P0-6 (t_88479cda). If the periodic sync thread died, writes
//!   were still acknowledged and never fsynced; a Batch fsync error and a
//!   Group flush error were logged and the write acknowledged anyway; a Group
//!   stall panicked the writer. All three now refuse the write, and a dead
//!   thread can be restarted by the node supervisor.
//!
//! Three strategies control when segment buffers are fsynced to disk:
//!
//! | Strategy | How it works | Acknowledged-but-unsynced window |
//! |----------|-------------|-----------------------------------|
//! | [`BatchSync`] | Fsync after every write | None: an fsync error fails the write |
//! | [`PeriodicSync`] | Background thread fsyncs on a timer | `max_delay` healthy; at most `sync_stall_deadline` otherwise |
//! | [`GroupSync`] | Writers wait for a background batch fsync | None: the writer fails after `sync_stall_deadline` |
//!
//! ## The periodic bound, stated
//!
//! Periodic acknowledges before the fsync (that is its throughput). The window
//! is bounded by refusing writes, not by trusting the thread:
//!
//! - the sync thread died (a panic): every write is refused at once;
//! - the last fsync attempt failed: every write is refused until one succeeds;
//! - the oldest write not yet covered by a successful fsync is older than
//!   `sync_stall_deadline`: every write is refused until a sync catches up.
//!
//! So an acknowledged write that never reaches disk was acknowledged within
//! `sync_stall_deadline` of the oldest unsynced write, and the node stops
//! acknowledging after that: a crash loses at most `sync_stall_deadline`
//! (default 2 s) of acknowledged writes, and `max_delay` (10 ms) while sync is
//! healthy. A refused write's entry may still be in the segment buffer and
//! reach disk later; like a write timeout, its outcome is unknown, never "acked".
//!
//! A supervisor reads [`SyncHealthSnapshot`]s and calls
//! [`SyncStrategy::restart`] to replace a dead thread; the new thread first
//! fsyncs everything the dead one left behind.

// Items are used by later tasks (CommitLog, integration tests); suppress
// dead-code warnings until those modules exist.
#![allow(dead_code)]

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use parking_lot::{Condvar, Mutex};

use super::config::CommitLogBatchConfig;
use super::segment::Segment;

/// A flush callback that the sync strategy invokes to fsync the current segment.
///
/// The `CommitLog` provides a closure that loads the active segment and calls
/// `flush_to_disk()`. This keeps sync strategies decoupled from segment
/// rotation.
pub type FlushCallback = Arc<dyn Fn() -> ferrosa_common::Result<()> + Send + Sync>;

/// What an acknowledgement of this write means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckPolicy {
    /// The write is acknowledged on return: refuse it unless the strategy
    /// can stand behind it (see the module docs).
    Durable,
    /// The caller fsyncs explicitly (`CommitLog::force_sync`) before it
    /// acknowledges anything, so sync health must not fail this append. Used
    /// by atomic batches, where a refusal partway through would leave a torn
    /// prefix in the log.
    CallerSyncs,
}

/// Controls when commit log segment buffers are fsynced to disk.
///
/// The methods form a lifecycle:
/// 1. [`start()`](SyncStrategy::start) — launch background work (if any).
/// 2. [`on_write()`](SyncStrategy::on_write) — called after each mutation.
/// 3. [`stop()`](SyncStrategy::stop) — clean shutdown, flush pending data.
///
/// [`health()`](SyncStrategy::health) and [`restart()`](SyncStrategy::restart)
/// are for the supervisor.
pub trait SyncStrategy: Send + Sync {
    /// Called after each mutation is written to the segment buffer.
    ///
    /// `Err` means the write must not be acknowledged; see [`AckPolicy`].
    fn on_write(
        &self,
        segment: &Segment,
        position: u64,
        bytes: u64,
        ack: AckPolicy,
    ) -> ferrosa_common::Result<()>;

    /// Start background sync work (if any).
    fn start(&self) -> ferrosa_common::Result<()>;

    /// Shut down cleanly. Fsync any pending data.
    fn stop(&self);

    /// The current health of the sync machinery.
    fn health(&self) -> SyncHealthSnapshot;

    /// Replace a dead sync thread. `Ok(false)` when there was nothing to
    /// restart (healthy, stopped, or no thread at all).
    fn restart(&self) -> ferrosa_common::Result<bool>;

    /// Make the sync thread panic at its next sync attempt.
    #[cfg(any(test, feature = "test-support"))]
    fn inject_panic(&self);
}

// ---------------------------------------------------------------------------
// SyncHealth
// ---------------------------------------------------------------------------

/// Process-wide count of writes refused because the commit log could not make
/// them durable.
static REFUSED_WRITES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn refused_writes_total() -> u64 {
    REFUSED_WRITES_TOTAL.load(Ordering::Relaxed)
}

/// Durability bookkeeping shared by the writers and the sync thread.
///
/// Every write takes a sequence number AFTER its entry is complete in the
/// segment buffer; a sync reads the highest number before it flushes, so a
/// successful flush covers every number up to that ticket. All atomics; the
/// orderings that matter are `SeqCst` (see [`SyncHealth::sync_succeeded`]).
pub struct SyncHealth {
    base: Instant,
    stall_deadline: Duration,
    written_seq: AtomicU64,
    durable_seq: AtomicU64,
    /// Nanoseconds since `base`, plus one, of the oldest write not yet covered
    /// by a successful sync; 0 when every write is durable.
    unsynced_since: AtomicU64,
    /// The sync thread died and has not been restarted.
    dead: AtomicBool,
    /// The last fsync attempt failed.
    failing: AtomicBool,
    panics: AtomicU64,
    sync_failures: AtomicU64,
    restarts: AtomicU64,
    last_failure: ArcSwapOption<String>,
    #[cfg(any(test, feature = "test-support"))]
    inject_panic: AtomicBool,
}

/// A point-in-time copy of [`SyncHealth`] for supervisors and metrics.
#[derive(Clone, Debug)]
pub struct SyncHealthSnapshot {
    /// Whether this strategy runs a background sync thread at all.
    pub has_sync_thread: bool,
    pub dead: bool,
    pub failing: bool,
    pub panics: u64,
    pub sync_failures: u64,
    pub restarts: u64,
    /// How long the oldest unsynced write has waited, if any is waiting.
    pub unsynced_for: Option<Duration>,
    pub stall_deadline: Duration,
    pub last_failure: Option<String>,
}

impl SyncHealthSnapshot {
    /// The oldest unsynced write has waited past the stall deadline.
    pub fn stalled(&self) -> bool {
        self.unsynced_for
            .is_some_and(|waited| waited >= self.stall_deadline)
    }

    /// Writes are being refused (or would be): dead, failing or stalled.
    pub fn impaired(&self) -> bool {
        self.dead || self.failing || self.stalled()
    }
}

/// The highest write sequence a sync attempt will cover, and when it was read.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SyncTicket {
    target: u64,
    at: Instant,
}

impl SyncHealth {
    pub fn new(stall_deadline: Duration) -> Self {
        assert!(
            !stall_deadline.is_zero(),
            "a zero stall deadline would refuse every write"
        );
        Self {
            base: Instant::now(),
            stall_deadline,
            written_seq: AtomicU64::new(0),
            durable_seq: AtomicU64::new(0),
            unsynced_since: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            failing: AtomicBool::new(false),
            panics: AtomicU64::new(0),
            sync_failures: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            last_failure: ArcSwapOption::empty(),
            #[cfg(any(test, feature = "test-support"))]
            inject_panic: AtomicBool::new(false),
        }
    }

    fn stamp(&self, now: Instant) -> u64 {
        let nanos = now.saturating_duration_since(self.base).as_nanos();
        nanos.min(u128::from(u64::MAX - 1)) as u64 + 1
    }

    /// Register a write whose entry is complete in the segment buffer.
    pub(crate) fn note_write(&self, now: Instant) -> u64 {
        let seq = self.written_seq.fetch_add(1, Ordering::SeqCst) + 1;
        // Already dirty: the older timestamp stands, which is the point.
        let _ = self.unsynced_since.compare_exchange(
            0,
            self.stamp(now),
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        seq
    }

    pub(crate) fn begin_sync(&self, now: Instant) -> SyncTicket {
        SyncTicket {
            target: self.written_seq.load(Ordering::SeqCst),
            at: now,
        }
    }

    /// A flush covering `ticket` succeeded. Returns `true` on the
    /// failing -> healthy edge.
    pub(crate) fn sync_succeeded(&self, ticket: SyncTicket) -> bool {
        self.durable_seq.fetch_max(ticket.target, Ordering::SeqCst);
        // Writes numbered past the ticket are still unsynced, and none of them
        // started before the ticket was read, so the ticket's instant is a
        // conservative age for them. The second check catches a write that
        // lands between the first check and the store: its own CAS may have
        // lost to the value being replaced.
        let next = if self.written_seq.load(Ordering::SeqCst) == ticket.target {
            0
        } else {
            self.stamp(ticket.at)
        };
        self.unsynced_since.store(next, Ordering::SeqCst);
        if next == 0 && self.written_seq.load(Ordering::SeqCst) != ticket.target {
            let _ = self.unsynced_since.compare_exchange(
                0,
                self.stamp(ticket.at),
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
        self.failing.swap(false, Ordering::AcqRel)
    }

    /// An fsync attempt failed. Returns `true` on the healthy -> failing edge.
    pub(crate) fn sync_failed(&self, error: &ferrosa_common::Error) -> bool {
        self.sync_failures.fetch_add(1, Ordering::Relaxed);
        self.last_failure
            .store(Some(Arc::new(format!("fsync failed: {error}"))));
        !self.failing.swap(true, Ordering::AcqRel)
    }

    pub(crate) fn mark_dead(&self, detail: String) {
        self.panics.fetch_add(1, Ordering::Relaxed);
        self.last_failure.store(Some(Arc::new(detail)));
        self.dead.store(true, Ordering::SeqCst);
    }

    /// Called by a freshly started sync thread before its first sync.
    fn mark_alive(&self) {
        self.dead.store(false, Ordering::SeqCst);
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    pub(crate) fn all_durable(&self) -> bool {
        self.durable_seq.load(Ordering::SeqCst) >= self.written_seq.load(Ordering::SeqCst)
    }

    pub(crate) fn durable_through(&self, seq: u64) -> bool {
        self.durable_seq.load(Ordering::SeqCst) >= seq
    }

    fn unsynced_for(&self, now: Instant) -> Option<Duration> {
        let since = self.unsynced_since.load(Ordering::SeqCst);
        (since != 0).then(|| {
            let since = self.base + Duration::from_nanos(since - 1);
            now.saturating_duration_since(since)
        })
    }

    fn last_failure_text(&self) -> String {
        self.last_failure
            .load_full()
            .map(|detail| detail.as_ref().clone())
            .unwrap_or_else(|| "no failure recorded".to_string())
    }

    fn refuse(&self, reason: String) -> ferrosa_common::Error {
        REFUSED_WRITES_TOTAL.fetch_add(1, Ordering::Relaxed);
        ferrosa_common::Error::CommitLogNotDurable { reason }
    }

    /// May a write that is acknowledged on return be acknowledged now?
    pub(crate) fn admit(&self, now: Instant) -> ferrosa_common::Result<()> {
        if self.is_dead() {
            return Err(self.refuse(format!(
                "the commit-log sync thread died ({}); writes are refused until it is restarted",
                self.last_failure_text()
            )));
        }
        if self.failing.load(Ordering::Acquire) {
            return Err(self.refuse(format!(
                "the last commit-log fsync failed ({}); writes are refused until one succeeds",
                self.last_failure_text()
            )));
        }
        if let Some(waited) = self.unsynced_for(now) {
            if waited >= self.stall_deadline {
                return Err(self.refuse(format!(
                    "no commit-log fsync has completed for {}ms, past the {}ms stall deadline",
                    waited.as_millis(),
                    self.stall_deadline.as_millis()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self, now: Instant, has_sync_thread: bool) -> SyncHealthSnapshot {
        SyncHealthSnapshot {
            has_sync_thread,
            dead: self.is_dead(),
            failing: self.failing.load(Ordering::Acquire),
            panics: self.panics.load(Ordering::Relaxed),
            sync_failures: self.sync_failures.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            unsynced_for: self.unsynced_for(now),
            stall_deadline: self.stall_deadline,
            last_failure: self
                .last_failure
                .load_full()
                .map(|detail| detail.as_ref().clone()),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn arm_injected_panic(&self) {
        self.inject_panic.store(true, Ordering::SeqCst);
    }

    /// Panic here if a test armed it. Compiled out of production builds.
    fn maybe_injected_panic(&self) {
        #[cfg(any(test, feature = "test-support"))]
        if self.inject_panic.swap(false, Ordering::SeqCst) {
            panic!("injected commit-log sync panic");
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

/// Spawn a sync thread whose panic marks `health` dead (so writes are refused)
/// and then runs `after_death` (to wake writers waiting on it).
fn spawn_sync_thread<B, D>(
    name: &'static str,
    health: Arc<SyncHealth>,
    body: B,
    after_death: D,
) -> ferrosa_common::Result<JoinHandle<()>>
where
    B: FnOnce() + Send + 'static,
    D: FnOnce() + Send + 'static,
{
    // Returning only after the new thread has cleared `dead` means a write
    // made right after a restart is not refused by the old thread's death.
    let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
    let handle = thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            health.mark_alive();
            // The receiver is gone only if the spawner gave up waiting, which
            // it reports below.
            let _ = started_tx.send(());
            // The body's state is atomics and parking_lot locks, which do not
            // poison; nothing is left half-updated that a restart would read.
            if let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(body)) {
                let message = panic_message(payload.as_ref());
                tracing::error!(
                    thread = name,
                    panic = %message,
                    "commit-log sync thread panicked; writes are refused until the supervisor restarts it"
                );
                health.mark_dead(format!("{name} panicked: {message}"));
                after_death();
            }
        })
        .map_err(|e| {
            ferrosa_common::Error::Io(std::io::Error::other(format!(
                "could not spawn the {name} thread: {e}"
            )))
        })?;
    match started_rx.recv_timeout(SYNC_THREAD_START_DEADLINE) {
        Ok(()) => Ok(handle),
        Err(e) => Err(ferrosa_common::Error::Io(std::io::Error::other(format!(
            "the {name} thread did not start within {}s: {e}",
            SYNC_THREAD_START_DEADLINE.as_secs()
        )))),
    }
}

/// How long a (re)start waits for the new sync thread to come alive.
const SYNC_THREAD_START_DEADLINE: Duration = Duration::from_secs(10);

/// Join a sync thread that has exited or is about to.
fn join_sync_thread(name: &'static str, handle: JoinHandle<()>) {
    if handle.join().is_err() {
        // `spawn_sync_thread` catches the body's panics, so this is a panic in
        // the catch handler itself.
        tracing::error!(
            thread = name,
            "commit-log sync thread died outside its panic handler"
        );
    }
}

/// Log the healthy -> failing edge at ERROR and every repeat at DEBUG.
fn report_sync_failure(strategy: &'static str, health: &SyncHealth, e: &ferrosa_common::Error) {
    if health.sync_failed(e) {
        tracing::error!(
            strategy,
            %e,
            "commit-log fsync failed; writes are refused until an fsync succeeds"
        );
    } else {
        tracing::debug!(strategy, %e, "commit-log fsync still failing");
    }
}

fn report_sync_success(strategy: &'static str, health: &SyncHealth, ticket: SyncTicket) {
    if health.sync_succeeded(ticket) {
        tracing::warn!(
            strategy,
            "commit-log fsync recovered; writes are acknowledged again"
        );
    }
}

// ---------------------------------------------------------------------------
// BatchSync
// ---------------------------------------------------------------------------

/// Fsyncs after every single write. Zero data loss, highest latency.
///
/// `on_write()` calls `segment.flush_to_disk()` synchronously, so every
/// mutation is durable before the writer returns, and an fsync error fails
/// the write. No background thread.
pub struct BatchSync {
    health: SyncHealth,
}

impl BatchSync {
    pub fn new() -> Self {
        Self {
            health: SyncHealth::new(CommitLogBatchConfig::DEFAULT_SYNC_STALL_DEADLINE),
        }
    }
}

impl Default for BatchSync {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncStrategy for BatchSync {
    fn on_write(
        &self,
        segment: &Segment,
        _position: u64,
        bytes: u64,
        ack: AckPolicy,
    ) -> ferrosa_common::Result<()> {
        if ack == AckPolicy::CallerSyncs {
            // The caller's force_sync is the fsync; an error here would tear
            // its batch.
            return Ok(());
        }
        observe_sync_batch(1, bytes, Duration::ZERO);
        let ticket = self.health.begin_sync(Instant::now());
        // No sync marker needed: BatchSync flushes every entry individually,
        // so every entry is already durable. Markers are only useful for
        // PeriodicSync/GroupSync where batches of entries are flushed together.
        match segment.flush_to_disk() {
            Ok(()) => {
                report_sync_success("batch", &self.health, ticket);
                Ok(())
            }
            Err(e) => {
                report_sync_failure("batch", &self.health, &e);
                Err(self.health.refuse(format!("fsync failed: {e}")))
            }
        }
    }

    fn start(&self) -> ferrosa_common::Result<()> {
        // No background thread needed.
        Ok(())
    }

    fn stop(&self) {
        // No-op: every write is already fsynced.
    }

    fn health(&self) -> SyncHealthSnapshot {
        self.health.snapshot(Instant::now(), false)
    }

    fn restart(&self) -> ferrosa_common::Result<bool> {
        Ok(false)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn inject_panic(&self) {
        panic!("BatchSync has no sync thread to inject a panic into");
    }
}

// ---------------------------------------------------------------------------
// PeriodicSync
// ---------------------------------------------------------------------------

/// Fsyncs on a timer. Best throughput; the ack-before-fsync window is bounded
/// as the module docs state.
///
/// `on_write()` does not wait for the fsync. A background thread wakes every
/// `sync_interval` (or when a batch fills) and calls the flush callback.
pub struct PeriodicSync {
    shared: Arc<PeriodicShared>,

    /// Background thread handle, protected by a mutex so `stop()` and
    /// `restart()` can take it.
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// State shared between `PeriodicSync` and its sync thread.
struct PeriodicShared {
    /// Interval between fsyncs.
    sync_interval: Duration,

    /// Flush callback provided at construction.
    flush_callback: FlushCallback,

    /// Signals the background thread to stop.
    stop_flag: AtomicBool,

    /// Condvar used to wake the background thread early on stop.
    wake: (Mutex<bool>, Condvar),

    /// Number of writes waiting for the next timed flush.
    pending: AtomicU64,

    /// Bytes waiting for the next timed flush.
    pending_bytes: AtomicU64,

    /// Adaptive batch controls.
    batch: CommitLogBatchConfig,

    health: Arc<SyncHealth>,
}

const PERIODIC_THREAD: &str = "commitlog-periodic-sync";

impl PeriodicSync {
    pub fn new(sync_interval: Duration, flush_callback: FlushCallback) -> Self {
        Self::with_batch(
            sync_interval,
            CommitLogBatchConfig::with_max_delay(sync_interval),
            flush_callback,
        )
    }

    pub fn with_batch(
        sync_interval: Duration,
        batch: CommitLogBatchConfig,
        flush_callback: FlushCallback,
    ) -> Self {
        let health = Arc::new(SyncHealth::new(batch.sync_stall_deadline));
        Self {
            shared: Arc::new(PeriodicShared {
                sync_interval,
                flush_callback,
                stop_flag: AtomicBool::new(false),
                wake: (Mutex::new(false), Condvar::new()),
                pending: AtomicU64::new(0),
                pending_bytes: AtomicU64::new(0),
                batch,
                health,
            }),
            handle: Mutex::new(None),
        }
    }

    fn spawn(&self) -> ferrosa_common::Result<JoinHandle<()>> {
        let shared = Arc::clone(&self.shared);
        spawn_sync_thread(
            PERIODIC_THREAD,
            Arc::clone(&self.shared.health),
            move || shared.run(),
            || {},
        )
    }

    fn stop_inner(&self, flush_final: bool) {
        let shared = &self.shared;
        shared.stop_flag.store(true, Ordering::Release);

        {
            let (lock, cvar) = &shared.wake;
            let mut stopped = lock.lock();
            *stopped = true;
            cvar.notify_one();
        }

        if let Some(handle) = self.handle.lock().take() {
            join_sync_thread(PERIODIC_THREAD, handle);
        }

        if flush_final {
            let ticket = shared.health.begin_sync(Instant::now());
            match (shared.flush_callback)() {
                Ok(()) => report_sync_success("periodic", &shared.health, ticket),
                Err(e) => {
                    report_sync_failure("periodic", &shared.health, &e);
                    tracing::error!(%e, "commitlog: shutdown flush_callback failed — data may not be durable");
                }
            }
        }
    }
}

impl PeriodicShared {
    /// Nothing to sync: no pending writes and every write covered.
    fn is_clean(&self) -> bool {
        self.pending.load(Ordering::Acquire) == 0 && self.health.all_durable()
    }

    fn run(&self) {
        while !self.stop_flag.load(Ordering::Acquire) {
            let (lock, cvar) = &self.wake;
            let mut stopped = lock.lock();
            if self.is_clean() {
                let result = cvar.wait_for(&mut stopped, self.sync_interval);
                if result.timed_out() && self.is_clean() {
                    PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }

            if self.stop_flag.load(Ordering::Acquire) {
                break;
            }

            let opened_at = Instant::now();
            loop {
                if self.pending_bytes.load(Ordering::Acquire) >= self.batch.target_bytes {
                    break;
                }
                let elapsed = opened_at.elapsed();
                if elapsed >= self.batch.max_delay {
                    break;
                }
                let remaining = self.batch.max_delay.saturating_sub(elapsed);
                // Timing out is the normal way out of the batch window.
                let _ = cvar.wait_for(&mut stopped, remaining.min(self.sync_interval));
                if self.stop_flag.load(Ordering::Acquire) {
                    break;
                }
            }
            drop(stopped);

            if self.stop_flag.load(Ordering::Acquire) {
                break;
            }

            let pending_writes = self.pending.swap(0, Ordering::AcqRel);
            let batch_bytes = self.pending_bytes.swap(0, Ordering::AcqRel);
            if pending_writes == 0 && self.health.all_durable() {
                PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            PENDING_WRITES.fetch_sub(pending_writes, Ordering::Relaxed);
            PENDING_BYTES.fetch_sub(batch_bytes, Ordering::Relaxed);
            self.sync_once(pending_writes, batch_bytes, opened_at);
        }
    }

    /// One fsync covering every write registered so far. A write left by a
    /// dead predecessor thread has no pending count but is not durable, so it
    /// is covered here too.
    fn sync_once(&self, pending_writes: u64, batch_bytes: u64, opened_at: Instant) {
        let ticket = self.health.begin_sync(Instant::now());
        self.health.maybe_injected_panic();
        match (self.flush_callback)() {
            Ok(()) => {
                report_sync_success("periodic", &self.health, ticket);
                observe_sync_batch(pending_writes, batch_bytes, opened_at.elapsed());
            }
            Err(e) => {
                self.pending.fetch_add(pending_writes, Ordering::AcqRel);
                self.pending_bytes.fetch_add(batch_bytes, Ordering::AcqRel);
                PENDING_WRITES.fetch_add(pending_writes, Ordering::Relaxed);
                PENDING_BYTES.fetch_add(batch_bytes, Ordering::Relaxed);
                report_sync_failure("periodic", &self.health, &e);
            }
        }
    }
}

impl SyncStrategy for PeriodicSync {
    fn on_write(
        &self,
        _segment: &Segment,
        _position: u64,
        bytes: u64,
        ack: AckPolicy,
    ) -> ferrosa_common::Result<()> {
        let shared = &self.shared;
        let now = Instant::now();
        shared.health.note_write(now);
        // Edge-trigger the sync thread when the log transitions from clean to
        // dirty. This is not a per-write stream: writes already covered by the
        // open batch only bump the counter, so they cannot interrupt the timer
        // and collapse batching into tiny fsyncs.
        let previous = shared.pending.fetch_add(1, Ordering::AcqRel);
        let previous_bytes = shared.pending_bytes.fetch_add(bytes, Ordering::AcqRel);
        PENDING_WRITES.fetch_add(1, Ordering::Relaxed);
        PENDING_BYTES.fetch_add(bytes, Ordering::Relaxed);
        if previous == 0 || previous_bytes.saturating_add(bytes) >= shared.batch.target_bytes {
            // Notify while holding `wake.lock`, for the same reason GroupSync
            // does: the sync thread holds this lock when it checks `pending`
            // and the batch's byte target before calling wait_for(). Without
            // the lock a notification sent in that window is lost and the
            // thread sleeps the whole sync interval, so a batch that reached
            // target_bytes is not fsynced until the timer fires.
            let (lock, cvar) = &shared.wake;
            let _guard = lock.lock();
            cvar.notify_one();
        }
        match ack {
            AckPolicy::Durable => shared.health.admit(now),
            AckPolicy::CallerSyncs => Ok(()),
        }
    }

    fn start(&self) -> ferrosa_common::Result<()> {
        let handle = self.spawn()?;
        *self.handle.lock() = Some(handle);
        Ok(())
    }

    fn stop(&self) {
        self.stop_inner(true);
    }

    fn health(&self) -> SyncHealthSnapshot {
        self.shared.health.snapshot(Instant::now(), true)
    }

    fn restart(&self) -> ferrosa_common::Result<bool> {
        if self.shared.stop_flag.load(Ordering::Acquire) || !self.shared.health.is_dead() {
            return Ok(false);
        }
        // Holding the handle slot serializes restarts: at most one sync
        // thread runs. The dead thread has already left its body.
        let mut slot = self.handle.lock();
        if !self.shared.health.is_dead() {
            return Ok(false);
        }
        if let Some(dead) = slot.take() {
            join_sync_thread(PERIODIC_THREAD, dead);
        }
        // The new thread clears `dead` itself before its first sync, so a
        // spawn failure leaves writes refused.
        let handle = self.spawn()?;
        *slot = Some(handle);
        self.shared.health.restarts.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn inject_panic(&self) {
        self.shared.health.arm_injected_panic();
    }
}

impl Drop for PeriodicSync {
    fn drop(&mut self) {
        self.stop_inner(false);
    }
}

static PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn periodic_idle_flush_skipped_total() -> u64 {
    PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// GroupSync
// ---------------------------------------------------------------------------

/// Fsyncs batches of writes. Bounded latency, good throughput.
///
/// Writers call `on_write()`, which registers the write, signals the
/// background thread, and blocks until a successful fsync covers the write.
/// The writer fails instead when the thread died or `sync_stall_deadline`
/// passes. The background thread wakes on a writer signal or `max_wait`,
/// calls the flush callback, then notifies all waiting writers.
pub struct GroupSync {
    shared: Arc<GroupShared>,

    /// Background thread handle.
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Shared coordination state between writers and the group sync thread.
struct GroupShared {
    /// Maximum time to wait before fsyncing a batch.
    max_wait: Duration,

    /// Adaptive batch controls.
    batch: CommitLogBatchConfig,

    /// Flush callback provided at construction.
    flush_callback: FlushCallback,

    /// Signals the background thread to stop.
    stop_flag: AtomicBool,

    /// Number of writes pending flush.
    pending: AtomicU64,

    /// Bytes pending flush.
    pending_bytes: AtomicU64,

    /// Condvar signaled by writers when new data is pending.
    writer_signal: (Mutex<()>, Condvar),

    /// Condvar signaled by the flush thread when a batch is complete (or the
    /// thread died). Always notified while holding its mutex: a waiter checks
    /// `durable_seq` under it, so a notify without it can be lost.
    flush_complete: (Mutex<()>, Condvar),

    health: Arc<SyncHealth>,
}

const GROUP_THREAD: &str = "commitlog-group-sync";

impl GroupSync {
    pub fn new(max_wait: Duration, flush_callback: FlushCallback) -> Self {
        Self::with_batch(
            max_wait,
            CommitLogBatchConfig::with_max_delay(max_wait),
            flush_callback,
        )
    }

    pub fn with_batch(
        max_wait: Duration,
        batch: CommitLogBatchConfig,
        flush_callback: FlushCallback,
    ) -> Self {
        let health = Arc::new(SyncHealth::new(batch.sync_stall_deadline));
        Self {
            shared: Arc::new(GroupShared {
                max_wait,
                batch,
                flush_callback,
                stop_flag: AtomicBool::new(false),
                pending: AtomicU64::new(0),
                pending_bytes: AtomicU64::new(0),
                writer_signal: (Mutex::new(()), Condvar::new()),
                flush_complete: (Mutex::new(()), Condvar::new()),
                health,
            }),
            handle: Mutex::new(None),
        }
    }

    fn spawn(&self) -> ferrosa_common::Result<JoinHandle<()>> {
        let shared = Arc::clone(&self.shared);
        let waker = Arc::clone(&self.shared);
        spawn_sync_thread(
            GROUP_THREAD,
            Arc::clone(&self.shared.health),
            move || shared.run(),
            move || waker.wake_writers(),
        )
    }

    fn stop_inner(&self, flush_final: bool) {
        let shared = &self.shared;
        {
            let (lock, cvar) = &shared.writer_signal;
            let _guard = lock.lock();
            shared.stop_flag.store(true, Ordering::Release);
            cvar.notify_all();
        }

        if let Some(handle) = self.handle.lock().take() {
            join_sync_thread(GROUP_THREAD, handle);
        }

        if flush_final {
            let ticket = shared.health.begin_sync(Instant::now());
            match (shared.flush_callback)() {
                Ok(()) => report_sync_success("group", &shared.health, ticket),
                Err(e) => {
                    report_sync_failure("group", &shared.health, &e);
                    tracing::error!(%e, "commitlog: shutdown flush_callback failed — data may not be durable");
                }
            }
        }
        shared.wake_writers();
    }
}

impl GroupShared {
    fn wake_writers(&self) {
        let (lock, cvar) = &self.flush_complete;
        let _guard = lock.lock();
        cvar.notify_all();
    }

    fn run(&self) {
        while !self.stop_flag.load(Ordering::Acquire) {
            let opened_at;
            // Wait for a writer signal or max_wait timeout.
            {
                let (lock, cvar) = &self.writer_signal;
                let mut guard = lock.lock();

                // Wait only if there is nothing to sync. A write left by a
                // dead predecessor has no pending count but is not durable.
                if self.pending.load(Ordering::Acquire) == 0 && self.health.all_durable() {
                    // Timing out is how an idle thread re-checks the stop flag.
                    let _timed_out = cvar.wait_for(&mut guard, self.max_wait);
                    if self.stop_flag.load(Ordering::Acquire) {
                        break;
                    }
                    if self.pending.load(Ordering::Acquire) == 0 && self.health.all_durable() {
                        // Timed out, a spurious wake, or a stop notification
                        // without writes.
                        continue;
                    }
                }

                opened_at = Instant::now();
                while self.pending_bytes.load(Ordering::Acquire) < self.batch.target_bytes {
                    if self.stop_flag.load(Ordering::Acquire) {
                        break;
                    }
                    let elapsed = opened_at.elapsed();
                    if elapsed >= self.batch.max_delay {
                        break;
                    }
                    let result =
                        cvar.wait_for(&mut guard, self.batch.max_delay.saturating_sub(elapsed));
                    if result.timed_out() || self.stop_flag.load(Ordering::Acquire) {
                        break;
                    }
                }
            }

            if self.stop_flag.load(Ordering::Acquire) {
                break;
            }

            let pending = self.pending.swap(0, Ordering::AcqRel);
            let batch_bytes = self.pending_bytes.swap(0, Ordering::AcqRel);
            PENDING_WRITES.fetch_sub(pending, Ordering::Relaxed);
            PENDING_BYTES.fetch_sub(batch_bytes, Ordering::Relaxed);
            let ticket = self.health.begin_sync(Instant::now());
            self.health.maybe_injected_panic();
            match (self.flush_callback)() {
                Ok(()) => {
                    report_sync_success("group", &self.health, ticket);
                    observe_sync_batch(pending, batch_bytes, opened_at.elapsed());
                }
                Err(e) => {
                    self.pending.fetch_add(pending, Ordering::AcqRel);
                    self.pending_bytes.fetch_add(batch_bytes, Ordering::AcqRel);
                    PENDING_WRITES.fetch_add(pending, Ordering::Relaxed);
                    PENDING_BYTES.fetch_add(batch_bytes, Ordering::Relaxed);
                    report_sync_failure("group", &self.health, &e);
                }
            }

            // Wake every waiting writer to re-check its own write.
            self.wake_writers();
        }
    }
}

impl SyncStrategy for GroupSync {
    fn on_write(
        &self,
        _segment: &Segment,
        _position: u64,
        bytes: u64,
        ack: AckPolicy,
    ) -> ferrosa_common::Result<()> {
        let shared = &self.shared;
        let started = Instant::now();
        let seq = shared.health.note_write(started);

        // Increment pending while holding writer_signal.lock.
        //
        // The flush thread holds this same lock when it checks `pending == 0`
        // before calling wait_for(). Holding the lock here closes the race:
        // either we increment before the flush thread checks (it sees > 0 and
        // skips the wait), or we increment while the flush thread is already
        // sleeping in wait_for (our notify_one wakes it). Without the lock,
        // a notification sent between the check and the wait is lost, causing
        // the flush thread to sleep the full max_wait before flushing.
        {
            let (lock, cvar) = &shared.writer_signal;
            let _guard = lock.lock();
            shared.pending.fetch_add(1, Ordering::AcqRel);
            shared.pending_bytes.fetch_add(bytes, Ordering::AcqRel);
            PENDING_WRITES.fetch_add(1, Ordering::Relaxed);
            PENDING_BYTES.fetch_add(bytes, Ordering::Relaxed);
            cvar.notify_one();
        }

        if ack == AckPolicy::CallerSyncs {
            return Ok(());
        }

        // Wait until a successful fsync covers this write. Every exit other
        // than that one refuses the write; before, a failed flush still woke
        // the writer as if it had succeeded, and a stall panicked it.
        let deadline = shared.health.stall_deadline;
        let (lock, cvar) = &shared.flush_complete;
        let mut guard = lock.lock();
        loop {
            if shared.health.durable_through(seq) {
                return Ok(());
            }
            if shared.health.is_dead() {
                return shared.health.admit(Instant::now());
            }
            let waited = started.elapsed();
            if waited >= deadline {
                return Err(shared.health.refuse(format!(
                    "no commit-log fsync covered this write within the {}ms stall deadline ({})",
                    deadline.as_millis(),
                    shared.health.last_failure_text()
                )));
            }
            // A timeout re-checks the deadline above.
            let _ = cvar.wait_for(&mut guard, deadline - waited);
        }
    }

    fn start(&self) -> ferrosa_common::Result<()> {
        let handle = self.spawn()?;
        *self.handle.lock() = Some(handle);
        Ok(())
    }

    fn stop(&self) {
        self.stop_inner(true);
    }

    fn health(&self) -> SyncHealthSnapshot {
        self.shared.health.snapshot(Instant::now(), true)
    }

    fn restart(&self) -> ferrosa_common::Result<bool> {
        if self.shared.stop_flag.load(Ordering::Acquire) || !self.shared.health.is_dead() {
            return Ok(false);
        }
        let mut slot = self.handle.lock();
        if !self.shared.health.is_dead() {
            return Ok(false);
        }
        if let Some(dead) = slot.take() {
            join_sync_thread(GROUP_THREAD, dead);
        }
        let handle = self.spawn()?;
        *slot = Some(handle);
        self.shared.health.restarts.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn inject_panic(&self) {
        self.shared.health.arm_injected_panic();
    }
}

impl Drop for GroupSync {
    fn drop(&mut self) {
        self.stop_inner(false);
    }
}

static SYNC_BATCHES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_BATCH_WRITES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_BATCH_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_BATCH_WAIT_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static SYNC_BATCH_WAIT_MICROS_MAX: AtomicU64 = AtomicU64::new(0);
static PENDING_WRITES: AtomicU64 = AtomicU64::new(0);
static PENDING_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) struct SyncBatchMetrics {
    pub batches: u64,
    pub writes: u64,
    pub bytes: u64,
    pub wait_micros_total: u64,
    pub wait_micros_max: u64,
    pub pending_writes: u64,
    pub pending_bytes: u64,
}

pub(crate) fn sync_batch_metrics() -> SyncBatchMetrics {
    SyncBatchMetrics {
        batches: SYNC_BATCHES_TOTAL.load(Ordering::Relaxed),
        writes: SYNC_BATCH_WRITES_TOTAL.load(Ordering::Relaxed),
        bytes: SYNC_BATCH_BYTES_TOTAL.load(Ordering::Relaxed),
        wait_micros_total: SYNC_BATCH_WAIT_MICROS_TOTAL.load(Ordering::Relaxed),
        wait_micros_max: SYNC_BATCH_WAIT_MICROS_MAX.load(Ordering::Relaxed),
        pending_writes: PENDING_WRITES.load(Ordering::Relaxed),
        pending_bytes: PENDING_BYTES.load(Ordering::Relaxed),
    }
}

fn observe_sync_batch(writes: u64, bytes: u64, wait: Duration) {
    SYNC_BATCHES_TOTAL.fetch_add(1, Ordering::Relaxed);
    SYNC_BATCH_WRITES_TOTAL.fetch_add(writes, Ordering::Relaxed);
    SYNC_BATCH_BYTES_TOTAL.fetch_add(bytes, Ordering::Relaxed);
    let micros = wait.as_micros().min(u64::MAX as u128) as u64;
    SYNC_BATCH_WAIT_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    update_max_u64(&SYNC_BATCH_WAIT_MICROS_MAX, micros);
}

fn update_max_u64(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::Relaxed);
    while value > current {
        match target.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "sync_durability_tests.rs"]
mod durability_tests;

#[cfg(test)]
mod tests {
    use super::super::segment::Segment;
    use super::*;

    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
    use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};

    use crate::commitlog::mutation::Mutation;

    /// Helper to create a simple mutation for testing.
    fn simple_mutation() -> Mutation {
        Mutation {
            mutation_id: [0x10u8; 16],
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            key: DecoratedKey::new(PartitionKey::new(b"pk1".to_vec())),
            rows: vec![Row {
                clustering: vec![1, 2, 3],
                cells: vec![(0, CellValue::live(b"hello".to_vec(), 1000))],
                deletion: DeletionTime::LIVE,
                primary_key_liveness: LivenessInfo::with_timestamp(1000),
            }],
            timestamp: 42_000,
        }
    }

    /// Write a mutation into a segment and return (segment, offset).
    fn write_mutation(dir: &std::path::Path) -> (Arc<Segment>, u64) {
        let segment = Arc::new(Segment::new(1, 4096, dir));
        let m = simple_mutation();
        let total_size = Segment::entry_total_size(&m);
        let offset = segment.allocate(total_size).unwrap();
        segment.write_entry(offset, &m);
        (segment, offset)
    }

    #[test]
    fn batch_sync_flushes_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        let sync = BatchSync::new();
        sync.start().expect("start the sync thread");
        sync.on_write(&segment, offset, 128, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");

        // After on_write, the file should exist on disk with the written data.
        // Note: on_write flushes then writes a sync marker, so the file
        // contains everything up to (but not including) the post-flush marker.
        let path = segment.path();
        assert!(path.exists(), "segment file should exist after batch sync");
        let contents = std::fs::read(path).unwrap();
        // File should contain at least the header + sync marker + entry.
        assert!(
            contents.len() > 25,
            "file should contain data beyond the header, got {} bytes",
            contents.len()
        );

        sync.stop();
    }

    #[test]
    fn periodic_sync_does_not_block() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        // Create a flush callback that does nothing (we just want to test
        // that on_write returns immediately).
        let flush_cb: FlushCallback = Arc::new(|| Ok(()));
        let sync = PeriodicSync::new(Duration::from_secs(60), flush_cb);
        sync.start().expect("start the sync thread");

        let start = Instant::now();
        sync.on_write(&segment, offset, 128, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");
        let elapsed = start.elapsed();

        // on_write should return in under 1ms (it does nothing).
        assert!(
            elapsed < Duration::from_millis(1),
            "periodic on_write should return immediately, took {:?}",
            elapsed
        );

        sync.stop();
    }

    #[test]
    fn periodic_sync_flushes_on_timer() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        let flush_observed = Arc::new((Mutex::new(false), Condvar::new()));
        let flush_observed_clone = Arc::clone(&flush_observed);
        let seg_clone = Arc::clone(&segment);
        let flush_cb: FlushCallback = Arc::new(move || {
            let result = seg_clone.flush_to_disk();
            if result.is_ok() {
                let (lock, cvar) = &*flush_observed_clone;
                *lock.lock() = true;
                cvar.notify_all();
            }
            result
        });
        let sync = PeriodicSync::new(Duration::from_millis(50), flush_cb);
        sync.start().expect("start the sync thread");
        sync.on_write(&segment, offset, 128, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");

        // The old test used a fixed 200ms sleep and then checked the file path.
        // Under full-package parallel test load, OS scheduling can delay the
        // background sync thread past that wall-clock window even though the
        // timer behavior is correct. Wait for the flush callback itself so the
        // assertion is synchronized to the event being tested, not scheduler
        // timing.
        // Wait on the predicate, not the notification: the flush can fire and
        // notify before this thread reaches the wait, and an unconditional
        // `wait_for` then sleeps the full timeout on a flag that is already set.
        let (lock, cvar) = &*flush_observed;
        let mut flushed = lock.lock();
        let result =
            cvar.wait_while_for(&mut flushed, |observed| !*observed, Duration::from_secs(5));
        assert!(
            *flushed && !result.timed_out(),
            "periodic flush callback did not run within 5s"
        );
        drop(flushed);

        let path = segment.path();
        assert!(
            path.exists(),
            "segment file should exist after periodic flush"
        );

        sync.stop();
    }

    #[test]
    fn periodic_sync_batches_write_notifications() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        let flush_count = Arc::new(AtomicUsize::new(0));
        let flush_count_clone = Arc::clone(&flush_count);
        let flush_cb: FlushCallback = Arc::new(move || {
            flush_count_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        let sync = PeriodicSync::new(Duration::from_millis(50), flush_cb);
        sync.start().expect("start the sync thread");

        for _ in 0..200 {
            sync.on_write(&segment, offset, 128, AckPolicy::Durable)
                .expect("a healthy sync strategy acknowledges the write");
        }

        thread::sleep(Duration::from_millis(140));
        sync.stop();

        let total_flushes = flush_count.load(Ordering::SeqCst);
        assert!(
            total_flushes <= 4,
            "periodic sync should batch write notifications; got {total_flushes} flushes"
        );
    }

    #[test]
    fn periodic_sync_flushes_when_target_bytes_reached() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        let flush_observed = Arc::new((Mutex::new(false), Condvar::new()));
        let flush_observed_clone = Arc::clone(&flush_observed);
        let flush_cb: FlushCallback = Arc::new(move || {
            let (lock, cvar) = &*flush_observed_clone;
            *lock.lock() = true;
            cvar.notify_all();
            Ok(())
        });
        let sync = PeriodicSync::with_batch(
            Duration::from_secs(3600),
            CommitLogBatchConfig {
                target_bytes: 4096,
                max_delay: Duration::from_secs(3600),
                sync_stall_deadline: CommitLogBatchConfig::DEFAULT_SYNC_STALL_DEADLINE,
            },
            flush_cb,
        );
        sync.start().expect("start the sync thread");
        sync.on_write(&segment, offset, 2048, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");
        sync.on_write(&segment, offset, 2048, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");

        let (lock, cvar) = &*flush_observed;
        let mut flushed = lock.lock();
        // Wait on the predicate, not the notification: the flush can fire and
        // notify before this thread reaches the wait, and an unconditional
        // `wait_for` then sleeps the full timeout on a flag that is already set.
        let result =
            cvar.wait_while_for(&mut flushed, |observed| !*observed, Duration::from_secs(5));
        assert!(
            *flushed && !result.timed_out(),
            "periodic sync did not flush after reaching target bytes"
        );
        drop(flushed);
        sync.stop();
    }

    #[test]
    fn group_sync_batches_writes() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, _) = write_mutation(dir.path());

        let flush_count = Arc::new(AtomicUsize::new(0));
        let flush_count_clone = Arc::clone(&flush_count);
        let seg_clone = Arc::clone(&segment);

        let flush_cb: FlushCallback = Arc::new(move || {
            flush_count_clone.fetch_add(1, Ordering::SeqCst);
            seg_clone.flush_to_disk()
        });

        let sync = Arc::new(GroupSync::new(Duration::from_millis(100), flush_cb));
        sync.start().expect("start the sync thread");

        // Spawn two writers that call on_write concurrently.
        let sync1 = Arc::clone(&sync);
        let seg1 = Arc::clone(&segment);
        let t1 = thread::spawn(move || {
            sync1
                .on_write(&seg1, 0, 128, AckPolicy::Durable)
                .expect("a healthy sync strategy acknowledges the write");
        });

        let sync2 = Arc::clone(&sync);
        let seg2 = Arc::clone(&segment);
        let t2 = thread::spawn(move || {
            sync2
                .on_write(&seg2, 0, 128, AckPolicy::Durable)
                .expect("a healthy sync strategy acknowledges the write");
        });

        t1.join().unwrap();
        t2.join().unwrap();

        sync.stop();

        // Both writes should have been batched into one flush (or possibly two
        // if timing is unlucky, but definitely fewer than one-per-write in the
        // common case). We allow 1-2 flushes from the background thread plus
        // the final flush in stop().
        let total_flushes = flush_count.load(Ordering::SeqCst);
        assert!(
            total_flushes <= 3,
            "expected batched flushes, got {total_flushes}"
        );
    }

    #[test]
    fn stop_flushes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, offset) = write_mutation(dir.path());

        let seg_clone = Arc::clone(&segment);
        let flush_cb: FlushCallback = Arc::new(move || seg_clone.flush_to_disk());

        // Use a very long interval so the periodic timer never fires during the test.
        let sync = PeriodicSync::new(Duration::from_secs(3600), flush_cb);
        sync.start().expect("start the sync thread");

        // on_write doesn't flush for PeriodicSync.
        sync.on_write(&segment, offset, 128, AckPolicy::Durable)
            .expect("a healthy sync strategy acknowledges the write");

        // File should not exist yet (timer hasn't fired).
        let path = segment.path();
        // It might or might not exist depending on thread scheduling, so we
        // just verify that after stop() it definitely exists.

        // stop() should do a final flush.
        sync.stop();

        assert!(
            path.exists(),
            "segment file must exist after stop() flushes pending data"
        );
        let contents = std::fs::read(path).unwrap();
        assert_eq!(
            contents.len(),
            segment.current_position() as usize,
            "file should contain all written data after stop()"
        );
    }

    #[test]
    fn group_sync_stop_flushes_pending() {
        let dir = tempfile::tempdir().unwrap();
        let (segment, _) = write_mutation(dir.path());

        let seg_clone = Arc::clone(&segment);
        let flush_cb: FlushCallback = Arc::new(move || seg_clone.flush_to_disk());

        // Use a very long max_wait so the group thread won't flush during the test
        // unless explicitly triggered by writes or stop.
        let sync = GroupSync::new(Duration::from_secs(3600), flush_cb);
        sync.start().expect("start the sync thread");

        // stop() should flush any pending data.
        sync.stop();

        let path = segment.path();
        assert!(
            path.exists(),
            "segment file must exist after GroupSync stop()"
        );
    }
}
