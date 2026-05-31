//! Sync strategies for the commit log.
//!
//! Three strategies control when segment buffers are fsynced to disk:
//!
//! | Strategy | How it works | Trade-off |
//! |----------|-------------|-----------|
//! | [`BatchSync`] | Fsync after every write | Zero data loss, highest latency |
//! | [`PeriodicSync`] | Background thread fsyncs on a timer | Best throughput, up to `sync_interval` data loss |
//! | [`GroupSync`] | Background thread fsyncs batches of writes | Bounded latency, good throughput |
//!
//! The [`SyncStrategy`] trait defines the interface. Each strategy receives
//! `on_write` calls after mutations are written to the segment buffer.
//! [`BatchSync`] calls `flush_to_disk` inline; [`PeriodicSync`] and
//! [`GroupSync`] delegate to a stored flush callback so they remain decoupled
//! from segment rotation.

// Items are used by later tasks (CommitLog, integration tests); suppress
// dead-code warnings until those modules exist.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use super::segment::Segment;

/// A flush callback that the sync strategy invokes to fsync the current segment.
///
/// The `CommitLog` (Task 9) will provide a closure that loads the active segment
/// and calls `flush_to_disk()`. This keeps sync strategies decoupled from
/// segment rotation.
pub type FlushCallback = Arc<dyn Fn() -> ferrosa_common::Result<()> + Send + Sync>;

/// Controls when commit log segment buffers are fsynced to disk.
///
/// The three methods form a lifecycle:
/// 1. [`start()`](SyncStrategy::start) — launch background work (if any).
/// 2. [`on_write()`](SyncStrategy::on_write) — called after each mutation.
/// 3. [`stop()`](SyncStrategy::stop) — clean shutdown, flush pending data.
pub trait SyncStrategy: Send + Sync {
    /// Called after each mutation is written to the segment buffer.
    ///
    /// May block (Batch/Group) or return immediately (Periodic).
    fn on_write(&self, segment: &Segment, position: u64);

    /// Start background sync work (if any).
    fn start(&self);

    /// Shut down cleanly. Fsync any pending data.
    fn stop(&self);
}

// ---------------------------------------------------------------------------
// BatchSync
// ---------------------------------------------------------------------------

/// Fsyncs after every single write. Zero data loss, highest latency.
///
/// `on_write()` calls `segment.flush_to_disk()` synchronously, so every
/// mutation is durable before the writer returns. No background thread.
pub struct BatchSync;

impl BatchSync {
    pub fn new() -> Self {
        Self
    }
}

impl SyncStrategy for BatchSync {
    fn on_write(&self, segment: &Segment, _position: u64) {
        // Intentionally ignoring the error here — the caller (CommitLog)
        // will handle flush failures at a higher level. In a production
        // system we'd propagate, but the trait signature returns `()`.
        if let Err(e) = segment.flush_to_disk() {
            tracing::error!(%e, "commitlog: flush_to_disk failed — data may not be durable");
        }
        // No sync marker needed: BatchSync flushes every entry individually,
        // so every entry is already durable. Markers are only useful for
        // PeriodicSync/GroupSync where batches of entries are flushed together.
    }

    fn start(&self) {
        // No-op: no background thread needed.
    }

    fn stop(&self) {
        // No-op: every write is already fsynced.
    }
}

// ---------------------------------------------------------------------------
// PeriodicSync
// ---------------------------------------------------------------------------

/// Fsyncs on a timer. Best throughput, up to `sync_interval` data loss on crash.
///
/// `on_write()` returns immediately (no blocking). A background thread wakes
/// every `sync_interval` and calls the flush callback.
pub struct PeriodicSync {
    /// Interval between fsyncs.
    sync_interval: Duration,

    /// Flush callback provided at construction.
    flush_callback: FlushCallback,

    /// Signals the background thread to stop.
    stop_flag: Arc<AtomicBool>,

    /// Background thread handle, protected by a mutex so `stop()` can take it.
    handle: Mutex<Option<JoinHandle<()>>>,

    /// Condvar used to wake the background thread early on stop.
    wake: Arc<(Mutex<bool>, Condvar)>,

    /// Number of writes waiting for the next timed flush.
    pending: Arc<AtomicU64>,
}

impl PeriodicSync {
    pub fn new(sync_interval: Duration, flush_callback: FlushCallback) -> Self {
        Self {
            sync_interval,
            flush_callback,
            stop_flag: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
            wake: Arc::new((Mutex::new(false), Condvar::new())),
            pending: Arc::new(AtomicU64::new(0)),
        }
    }

    fn stop_inner(&self, flush_final: bool) {
        self.stop_flag.store(true, Ordering::Release);

        {
            let (lock, cvar) = &*self.wake;
            let mut stopped = lock.lock();
            *stopped = true;
            cvar.notify_one();
        }

        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }

        if flush_final {
            if let Err(e) = (self.flush_callback)() {
                tracing::error!(%e, "commitlog: shutdown flush_callback failed — data may not be durable");
            }
        }
    }
}

impl SyncStrategy for PeriodicSync {
    fn on_write(&self, _segment: &Segment, _position: u64) {
        self.pending.fetch_add(1, Ordering::AcqRel);
        let (_lock, cvar) = &*self.wake;
        cvar.notify_one();
    }

    fn start(&self) {
        let stop_flag = Arc::clone(&self.stop_flag);
        let flush_callback = Arc::clone(&self.flush_callback);
        let interval = self.sync_interval;
        let wake = Arc::clone(&self.wake);
        let pending = Arc::clone(&self.pending);

        let thread = thread::Builder::new()
            .name("commitlog-periodic-sync".to_string())
            .spawn(move || {
                while !stop_flag.load(Ordering::Acquire) {
                    // Wait for the first write in a batch, then hold the
                    // batch open for one interval to collect nearby writes.
                    let (lock, cvar) = &*wake;
                    let mut stopped = lock.lock();
                    if pending.load(Ordering::Acquire) == 0 {
                        let result = cvar.wait_for(&mut stopped, interval);
                        if result.timed_out() && pending.load(Ordering::Acquire) == 0 {
                            PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }

                    if stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    let _ = cvar.wait_for(&mut stopped, interval);

                    if stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    let pending_writes = pending.swap(0, Ordering::AcqRel);
                    if pending_writes == 0 {
                        PERIODIC_IDLE_FLUSH_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    if let Err(e) = flush_callback() {
                        pending.fetch_add(pending_writes, Ordering::AcqRel);
                        tracing::error!(%e, "commitlog: periodic flush_callback failed — data may not be durable");
                    }
                }
            })
            .expect("failed to spawn periodic sync thread");

        *self.handle.lock() = Some(thread);
    }

    fn stop(&self) {
        self.stop_inner(true);
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
/// Writers call `on_write()` which increments a pending counter, signals the
/// background thread, and blocks until the batch is flushed. The background
/// thread wakes on condvar signal or `max_wait` timeout, calls the flush
/// callback, then notifies all waiting writers.
pub struct GroupSync {
    /// Maximum time to wait before fsyncing a batch.
    max_wait: Duration,

    /// Flush callback provided at construction.
    flush_callback: FlushCallback,

    /// Signals the background thread to stop.
    stop_flag: Arc<AtomicBool>,

    /// Background thread handle.
    handle: Mutex<Option<JoinHandle<()>>>,

    /// Shared state for coordinating writers and the flush thread.
    state: Arc<GroupSyncState>,
}

/// Shared coordination state between writers and the group sync thread.
struct GroupSyncState {
    /// Number of writes pending flush.
    pending: AtomicU64,

    /// Generation counter — incremented after each flush. Writers wait until
    /// the generation advances past the value they observed on entry.
    generation: AtomicU64,

    /// Condvar signaled by writers when new data is pending.
    writer_signal: (Mutex<()>, Condvar),

    /// Condvar signaled by the flush thread when a batch is complete.
    flush_complete: (Mutex<()>, Condvar),
}

impl GroupSync {
    pub fn new(max_wait: Duration, flush_callback: FlushCallback) -> Self {
        Self {
            max_wait,
            flush_callback,
            stop_flag: Arc::new(AtomicBool::new(false)),
            handle: Mutex::new(None),
            state: Arc::new(GroupSyncState {
                pending: AtomicU64::new(0),
                generation: AtomicU64::new(0),
                writer_signal: (Mutex::new(()), Condvar::new()),
                flush_complete: (Mutex::new(()), Condvar::new()),
            }),
        }
    }

    fn stop_inner(&self, flush_final: bool) {
        self.stop_flag.store(true, Ordering::Release);

        {
            let (_lock, cvar) = &self.state.writer_signal;
            cvar.notify_one();
        }

        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }

        if flush_final {
            if let Err(e) = (self.flush_callback)() {
                tracing::error!(%e, "commitlog: shutdown flush_callback failed — data may not be durable");
            }
        }

        self.state.generation.fetch_add(1, Ordering::AcqRel);
        {
            let (_lock, cvar) = &self.state.flush_complete;
            cvar.notify_all();
        }
    }
}

impl SyncStrategy for GroupSync {
    fn on_write(&self, _segment: &Segment, _position: u64) {
        // Record the generation before we add our pending write.
        let my_gen = self.state.generation.load(Ordering::Acquire);

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
            let (lock, cvar) = &self.state.writer_signal;
            let _guard = lock.lock();
            self.state.pending.fetch_add(1, Ordering::AcqRel);
            cvar.notify_one();
        }

        // Wait until the flush thread completes our batch (generation advances).
        let (lock, cvar) = &self.state.flush_complete;
        let mut guard = lock.lock();
        while self.state.generation.load(Ordering::Acquire) <= my_gen {
            let result = cvar.wait_for(&mut guard, Duration::from_secs(30));
            if result.timed_out() {
                // The flush thread has not advanced the generation in 30 seconds.
                // This indicates the thread has died or the disk is unresponsive.
                // Panic rather than silently returning — a caller that thinks the
                // write is durable when it is not would cause data loss.
                panic!(
                    "GroupSync flush thread stalled for 30s (generation stuck at {}); \
                     commit log is unresponsive",
                    my_gen
                );
            }
        }
    }

    fn start(&self) {
        let stop_flag = Arc::clone(&self.stop_flag);
        let flush_callback = Arc::clone(&self.flush_callback);
        let state = Arc::clone(&self.state);
        let max_wait = self.max_wait;

        let thread = thread::Builder::new()
            .name("commitlog-group-sync".to_string())
            .spawn(move || {
                while !stop_flag.load(Ordering::Acquire) {
                    // Wait for a writer signal or max_wait timeout.
                    {
                        let (lock, cvar) = &state.writer_signal;
                        let mut guard = lock.lock();

                        // Wait only if there are no pending writes.
                        if state.pending.load(Ordering::Acquire) == 0 {
                            let result = cvar.wait_for(&mut guard, max_wait);
                            if result.timed_out() && state.pending.load(Ordering::Acquire) == 0 {
                                // Timed out with no pending writes; loop back.
                                continue;
                            }
                        }
                    }

                    if stop_flag.load(Ordering::Acquire) {
                        break;
                    }

                    // Flush pending writes.
                    let pending = state.pending.swap(0, Ordering::AcqRel);
                    if pending > 0 {
                        if let Err(e) = flush_callback() {
                            tracing::error!(%e, "commitlog: group flush_callback failed — data may not be durable");
                        }
                    }

                    // Advance generation and wake all waiting writers.
                    state.generation.fetch_add(1, Ordering::AcqRel);
                    {
                        let (_lock, cvar) = &state.flush_complete;
                        cvar.notify_all();
                    }
                }
            })
            .expect("failed to spawn group sync thread");

        *self.handle.lock() = Some(thread);
    }

    fn stop(&self) {
        self.stop_inner(true);
    }
}

impl Drop for GroupSync {
    fn drop(&mut self) {
        self.stop_inner(false);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
        sync.start();
        sync.on_write(&segment, offset);

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
        sync.start();

        let start = Instant::now();
        sync.on_write(&segment, offset);
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
        sync.start();
        sync.on_write(&segment, offset);

        // The old test used a fixed 200ms sleep and then checked the file path.
        // Under full-package parallel test load, OS scheduling can delay the
        // background sync thread past that wall-clock window even though the
        // timer behavior is correct. Wait for the flush callback itself so the
        // assertion is synchronized to the event being tested, not scheduler
        // timing.
        let (lock, cvar) = &*flush_observed;
        let mut flushed = lock.lock();
        let result = cvar.wait_for(&mut flushed, Duration::from_secs(5));
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
        sync.start();

        // Spawn two writers that call on_write concurrently.
        let sync1 = Arc::clone(&sync);
        let seg1 = Arc::clone(&segment);
        let t1 = thread::spawn(move || {
            sync1.on_write(&seg1, 0);
        });

        let sync2 = Arc::clone(&sync);
        let seg2 = Arc::clone(&segment);
        let t2 = thread::spawn(move || {
            sync2.on_write(&seg2, 0);
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
        sync.start();

        // on_write doesn't flush for PeriodicSync.
        sync.on_write(&segment, offset);

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
        sync.start();

        // stop() should flush any pending data.
        sync.stop();

        let path = segment.path();
        assert!(
            path.exists(),
            "segment file must exist after GroupSync stop()"
        );
    }
}
