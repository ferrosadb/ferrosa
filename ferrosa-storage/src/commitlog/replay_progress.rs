//! Module: Report commit-log replay progress and retire replayed segments.
//! Correctness: Correct when a replay that runs for minutes says so at a
//! bounded interval, and a replayed segment is unlinked only after the barrier
//! that made its re-logged mutations durable, never on the replay thread.
//! Last revised: 2026-10-10
//! Last changed: Added (outage 2026-10-10, F4/D5).
//!
//! On 2026-10-10 a node spent minutes in commit-log replay with no log line
//! at all, and an operator restarted it mid-replay. [`ReplayProgress`] is the
//! surface that makes replay visible: the engine logs every report, tests
//! observe them directly.

use std::path::PathBuf;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// How often a replay reports progress by default.
pub const REPLAY_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// Name of the thread that unlinks replayed segments.
pub const RETIRE_THREAD_NAME: &str = "commitlog-retire";

/// Where a commit-log replay stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayProgressReport {
    /// Segments fully processed (replayed and barrier-synced, or skipped as torn).
    pub segments_done: usize,
    /// Segments found on disk when replay started.
    pub segments_total: usize,
    /// On-disk bytes of the processed segments.
    pub bytes_done: u64,
    /// On-disk bytes of every segment found when replay started.
    pub bytes_total: u64,
    /// Mutations delivered to the replay callback.
    pub mutations: u64,
    /// Mutations dropped because a table checkpoint already covered them.
    pub dominated: u64,
    /// Time since replay started.
    pub elapsed: Duration,
}

/// Observer of a commit-log replay.
///
/// `progress` fires at most once per [`interval`](Self::interval), after a
/// segment completes; `finished` fires exactly once when replay succeeds.
pub trait ReplayProgress {
    /// Minimum time between two `progress` reports.
    fn interval(&self) -> Duration {
        REPLAY_PROGRESS_INTERVAL
    }

    /// Replay is still running.
    fn progress(&mut self, report: &ReplayProgressReport);

    /// Replay finished; `report` holds the totals.
    fn finished(&mut self, report: &ReplayProgressReport);
}

/// The [`ReplayProgress`] a node runs with: one INFO line per report.
#[derive(Debug, Default)]
pub struct LogReplayProgress;

impl ReplayProgress for LogReplayProgress {
    fn progress(&mut self, r: &ReplayProgressReport) {
        tracing::info!(
            segments_done = r.segments_done,
            segments_total = r.segments_total,
            bytes_done = r.bytes_done,
            bytes_total = r.bytes_total,
            mutations = r.mutations,
            dominated = r.dominated,
            elapsed_ms = r.elapsed.as_millis() as u64,
            "commitlog: replay progress {}/{} segments",
            r.segments_done,
            r.segments_total
        );
    }

    fn finished(&mut self, r: &ReplayProgressReport) {
        tracing::info!(
            segments = r.segments_total,
            bytes = r.bytes_total,
            mutations = r.mutations,
            dominated = r.dominated,
            elapsed_ms = r.elapsed.as_millis() as u64,
            "commitlog: replay finished: {} segments, {} mutations",
            r.segments_total,
            r.mutations
        );
    }
}

/// Counts a replay and reports it to an optional [`ReplayProgress`].
pub(crate) struct ReplayTally<'p> {
    progress: Option<&'p mut dyn ReplayProgress>,
    segment_bytes: Vec<u64>,
    started: Instant,
    last_report: Instant,
    report: ReplayProgressReport,
}

impl<'p> ReplayTally<'p> {
    /// Size every segment up front so each report carries the totals.
    pub(crate) fn start(
        progress: Option<&'p mut dyn ReplayProgress>,
        segments: &[(u64, PathBuf)],
    ) -> ferrosa_common::Result<Self> {
        let segment_bytes = segments
            .iter()
            .map(|(_, path)| std::fs::metadata(path).map(|m| m.len()))
            .collect::<std::io::Result<Vec<u64>>>()?;
        let started = Instant::now();
        Ok(Self {
            progress,
            report: ReplayProgressReport {
                segments_done: 0,
                segments_total: segments.len(),
                bytes_done: 0,
                bytes_total: segment_bytes.iter().sum(),
                mutations: 0,
                dominated: 0,
                elapsed: Duration::ZERO,
            },
            segment_bytes,
            started,
            last_report: started,
        })
    }

    pub(crate) fn delivered(&mut self) {
        self.report.mutations += 1;
    }

    pub(crate) fn dominated(&mut self) {
        self.report.dominated += 1;
    }

    /// Segment `index` is through its barrier (or was skipped as torn).
    pub(crate) fn segment_done(&mut self, index: usize) {
        debug_assert_eq!(
            index, self.report.segments_done,
            "segments complete in order"
        );
        self.report.segments_done += 1;
        self.report.bytes_done += self.segment_bytes[index];
        let Some(progress) = self.progress.as_deref_mut() else {
            return;
        };
        let now = Instant::now();
        if now.duration_since(self.last_report) >= progress.interval() {
            self.last_report = now;
            self.report.elapsed = now.duration_since(self.started);
            progress.progress(&self.report);
        }
    }

    /// Replay succeeded: emit the summary.
    pub(crate) fn finish(mut self) {
        debug_assert_eq!(self.report.segments_done, self.report.segments_total);
        self.report.elapsed = self.started.elapsed();
        if let Some(progress) = self.progress.as_deref_mut() {
            progress.finished(&self.report);
        }
    }
}

/// Unlinks replayed segment files on a dedicated thread.
///
/// The replay loop hands a path over only after its durability barrier
/// (`force_sync` + `segment_done`), so ordering is unchanged from the old
/// inline unlink; what moves is the cost of the unlink itself. A crash before
/// the unlink leaves a segment whose contents were also re-logged; the next
/// replay delivers both copies, which the engine's mutation-id dedup and
/// last-write-wins merge make harmless.
pub(crate) struct SegmentRetirer {
    tx: Option<mpsc::Sender<PathBuf>>,
    worker: Option<JoinHandle<()>>,
}

impl SegmentRetirer {
    /// Start the retire thread.
    pub(crate) fn spawn() -> ferrosa_common::Result<Self> {
        let (tx, rx) = mpsc::channel::<PathBuf>();
        let worker = std::thread::Builder::new()
            .name(RETIRE_THREAD_NAME.into())
            .spawn(move || {
                for path in rx {
                    retire_one(&path);
                }
            })?;
        Ok(Self {
            tx: Some(tx),
            worker: Some(worker),
        })
    }

    /// Queue `path` for unlinking. Fails only if the retire thread is gone,
    /// which leaves the segment on disk for the next replay.
    pub(crate) fn retire(&self, path: PathBuf) -> ferrosa_common::Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or_else(|| retire_error("commitlog: segment retirer already finished".into()))?;
        tx.send(path).map_err(|e| {
            retire_error(format!(
                "commitlog: segment retire thread is gone; {} stays on disk",
                e.0.display()
            ))
        })
    }

    /// Wait for every queued unlink. Errors only if the retire thread panicked.
    pub(crate) fn finish(mut self) -> ferrosa_common::Result<()> {
        self.join()
    }

    fn join(&mut self) -> ferrosa_common::Result<()> {
        drop(self.tx.take());
        match self.worker.take() {
            Some(worker) => worker.join().map_err(|_| {
                retire_error(
                    "commitlog: segment retire thread panicked; replayed segments may remain \
                     on disk and will replay again"
                        .into(),
                )
            }),
            None => Ok(()),
        }
    }
}

impl Drop for SegmentRetirer {
    /// A replay that failed part way still waits for the unlinks it queued,
    /// so no thread outlives the replay that started it.
    fn drop(&mut self) {
        if let Err(e) = self.join() {
            tracing::error!(%e, "commitlog: segment retirer");
        }
    }
}

fn retire_error(msg: String) -> ferrosa_common::Error {
    ferrosa_common::Error::Io(std::io::Error::other(msg))
}

pub(crate) fn retire_one(path: &std::path::Path) {
    #[cfg(test)]
    if test_trace::suppressed(path) {
        return;
    }
    match std::fs::remove_file(path) {
        Ok(()) => {
            #[cfg(test)]
            test_trace::record(test_trace::Event::Unlinked {
                path: path.to_path_buf(),
                thread: std::thread::current().name().map(str::to_string),
            });
        }
        // A segment left behind replays again next start, harmlessly.
        Err(e) => {
            tracing::warn!(%e, path = %path.display(), "commitlog: failed to remove segment file")
        }
    }
}

/// Test-only record of the replay barrier and the unlinks, keyed by path so
/// parallel tests do not see each other.
#[cfg(test)]
pub(crate) mod test_trace {
    use std::path::{Path, PathBuf};

    use parking_lot::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Event {
        /// The replay barrier completed for this segment.
        Synced { path: PathBuf },
        /// The segment was unlinked on `thread`.
        Unlinked {
            path: PathBuf,
            thread: Option<String>,
        },
    }

    static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());
    static SUPPRESSED_DIRS: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

    pub(crate) fn record(event: Event) {
        EVENTS.lock().push(event);
    }

    /// Events whose path lies under `dir`, in order.
    pub(crate) fn events_under(dir: &Path) -> Vec<Event> {
        EVENTS
            .lock()
            .iter()
            .filter(|e| match e {
                Event::Synced { path } | Event::Unlinked { path, .. } => path.starts_with(dir),
            })
            .cloned()
            .collect()
    }

    /// Simulate a crash between replay and unlink: segments under `dir` are
    /// never removed until [`unsuppress`] is called.
    pub(crate) fn suppress(dir: &Path) {
        SUPPRESSED_DIRS.lock().push(dir.to_path_buf());
    }

    pub(crate) fn unsuppress(dir: &Path) {
        SUPPRESSED_DIRS.lock().retain(|d| d != dir);
    }

    pub(crate) fn suppressed(path: &Path) -> bool {
        SUPPRESSED_DIRS.lock().iter().any(|d| path.starts_with(d))
    }
}
