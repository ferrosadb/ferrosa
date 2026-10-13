//! Module: Make every exit of the ferrosa process leave a last line.
//! Correctness: Correct when a panic on any thread, every fatal exit and every
//! orderly exit write a final line synchronously to the log file and stderr,
//! after the non-blocking log buffer has been flushed.
//! Last revised: 2026-10-10
//! Last changed: Added (outage 2026-10-10, F3/D4).
//!
//! On 2026-10-10 node2 and node3 died with no final log line. The tracing
//! writer is `tracing_appender::non_blocking`: lines queue in memory for a
//! worker thread, and `process::exit` / `abort()` skip the guard that flushes
//! them, so the lines that would have explained the death were the ones lost.
//!
//! This module owns that guard. [`fatal_exit`] and [`fatal_abort`] flush it
//! before leaving, and [`write_line`] bypasses it entirely: it opens the log
//! file in append mode and writes one line with an ordinary blocking write, so
//! it cannot be lost to a buffer that never drains.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, WorkerGuard};

/// Where the synchronous last line goes, and the guard of the buffered writer.
struct Sink {
    /// The rotating log file, or `None` when the process logs to stdout.
    log_file: Option<PathBuf>,
    guard: Mutex<Option<WorkerGuard>>,
}

static SINK: OnceLock<Sink> = OnceLock::new();

/// Hidden, test-only: microseconds the log writer sleeps per line, to make the
/// node's log drain as slowly as it does on a loaded machine
/// (`tests/last_line.rs`). Never set in production.
pub const TEST_LOG_WRITE_DELAY_ENV: &str = "FERROSA_TEST_LOG_WRITE_DELAY_US";

/// Lines handed to the non-blocking writer (accepted or dropped).
static LINES_QUEUED: AtomicU64 = AtomicU64::new(0);
/// Lines the writer thread has finished with (written or failed).
static LINES_DONE: AtomicU64 = AtomicU64::new(0);
/// Lines the writer thread failed to write.
static LINE_WRITE_ERRORS: AtomicU64 = AtomicU64::new(0);
/// The non-blocking writer's count of lines dropped because its buffer was full.
static LINES_DROPPED: OnceLock<ErrorCounter> = OnceLock::new();

/// The writer thread's end: counts every line it finishes, so an exit can
/// wait until everything queued before it has reached the file.
pub struct CountingWriter<W> {
    inner: W,
    delay: Option<Duration>,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    /// The writer thread calls this once per queued line.
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if let Some(delay) = self.delay {
            std::thread::sleep(delay);
        }
        let written = self.inner.write_all(buf);
        if written.is_err() {
            LINE_WRITE_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
        LINES_DONE.fetch_add(1, Ordering::Release);
        written
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// The emitting end: counts every line handed to the non-blocking writer.
#[derive(Clone)]
pub struct CountingQueue {
    inner: NonBlocking,
}

impl Write for CountingQueue {
    /// `tracing-subscriber` hands over one formatted event per call.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        LINES_QUEUED.fetch_add(1, Ordering::AcqRel);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CountingQueue {
    type Writer = CountingQueue;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// `tracing_appender::non_blocking`, with both ends counted so
/// [`flush_log_buffer`] can tell when the queue has drained.
pub fn non_blocking<W: Write + Send + 'static>(writer: W) -> (CountingQueue, WorkerGuard) {
    let delay = std::env::var(TEST_LOG_WRITE_DELAY_ENV)
        .ok()
        .map(|us| match us.parse::<u64>() {
            Ok(us) => Duration::from_micros(us),
            Err(e) => panic!("{TEST_LOG_WRITE_DELAY_ENV}={us:?}: {e}"),
        });
    let (inner, guard) = tracing_appender::non_blocking(CountingWriter {
        inner: writer,
        delay,
    });
    // One writer per process; a second call keeps the first counter.
    let _ = LINES_DROPPED.set(inner.error_counter());
    (CountingQueue { inner }, guard)
}

/// Take ownership of the non-blocking writer's guard. `log_file` is the file
/// the writer appends to, or `None` when the log is stdout.
///
/// Call once, right after the subscriber is installed.
pub fn install(log_file: Option<PathBuf>, guard: WorkerGuard) {
    let fresh = SINK.set(Sink {
        log_file,
        guard: Mutex::new(Some(guard)),
    });
    assert!(fresh.is_ok(), "lifecycle::install called twice");
}

/// Install the process-wide panic hook: a panic on ANY thread writes a FATAL
/// line synchronously, then the previous hook runs as before. Install it right
/// after [`install`]; hooks installed later (the consensus runtime's) wrap it.
pub fn install_panic_hook() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        let name = thread.name().unwrap_or("unnamed");
        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());
        // The main thread's panic unwinds out of `main` and ends the process:
        // drain the buffer first, so the lines before the panic land before it.
        if name == "main" {
            flush_log_buffer();
        }
        write_line(&format!(
            "FATAL: thread '{name}' panicked at {location}: {}",
            panic_payload(info)
        ));
        previous(info);
    }));
}

/// The message a panic carried, or a marker when it is not a string.
pub fn panic_payload<'a>(info: &'a std::panic::PanicHookInfo<'_>) -> &'a str {
    info.payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| info.payload().downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// Log `reason`, flush the buffered log, write `ferrosa exiting code=N
/// reason=...` synchronously, and exit with `code`. The replacement for every
/// `std::process::exit` in the binary.
pub fn fatal_exit(code: i32, reason: &str) -> ! {
    tracing::error!(code, reason, "fatal: exiting");
    exit_process(code, reason)
}

/// Like [`fatal_exit`] for paths that must `abort()` (no destructors, a core
/// dump where enabled): the line is flushed and written first.
pub fn fatal_abort(reason: &str) -> ! {
    tracing::error!(reason, "fatal: aborting");
    flush_log_buffer();
    write_line(&format!("FATAL: {reason}"));
    write_line("ferrosa exiting code=abort reason=see the FATAL line above");
    std::process::abort()
}

/// Flush the buffered log, write the exit line synchronously, then exit. The
/// orderly end of `main` goes through here too, so every exit's last line is
/// `ferrosa exiting code=N`.
pub fn exit_process(code: i32, reason: &str) -> ! {
    flush_log_buffer();
    write_line(&format!("ferrosa exiting code={code} reason={reason}"));
    std::process::exit(code)
}

/// How long an exit waits for the log queue to drain before it gives up and
/// says how much was lost.
const LOG_DRAIN_DEADLINE: Duration = Duration::from_secs(30);

/// Drain the non-blocking writer COMPLETELY, then stop it. Later `tracing`
/// events are discarded, so call this only on the way out.
///
/// Dropping the `WorkerGuard` alone is not a drain: tracing-appender 0.2.5
/// waits at most 100 ms to queue its shutdown and 1 s for the writer thread,
/// then returns silently. Under load (the full workspace test run) that kept
/// ~209 of 20,000 queued lines. So this waits, up to [`LOG_DRAIN_DEADLINE`],
/// until the writer thread has finished every line queued before the call,
/// and reports any shortfall and any lines the buffer dropped.
fn flush_log_buffer() {
    let Some(sink) = SINK.get() else {
        return; // before tracing started: nothing is buffered
    };
    // A poisoned lock means a panic while flushing; the guard is still ours.
    let guard = match sink.guard.lock() {
        Ok(mut slot) => slot.take(),
        Err(poisoned) => poisoned.into_inner().take(),
    };
    let Some(guard) = guard else {
        return; // already drained by an earlier exit path
    };
    let outcome = wait_for_drain(LOG_DRAIN_DEADLINE);
    drop(guard);
    if let Some(problem) = outcome.problem() {
        write_line(&problem);
    }
}

/// What the drain achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Drain {
    /// Lines queued when the drain started.
    queued: u64,
    /// Lines the writer finished (written or failed) by the end.
    done: u64,
    /// Lines the buffer dropped (it was full).
    dropped: u64,
    /// Lines the writer failed to write.
    write_errors: u64,
    waited: Duration,
}

impl Drain {
    fn complete(&self) -> bool {
        self.done + self.dropped >= self.queued
    }

    /// The line to report, if anything went missing.
    fn problem(&self) -> Option<String> {
        let mut parts = Vec::new();
        if !self.complete() {
            parts.push(format!(
                "log drain incomplete after {:?}: {} of {} queued lines not written",
                self.waited,
                self.queued - self.done - self.dropped,
                self.queued
            ));
        }
        if self.dropped > 0 {
            parts.push(format!(
                "log drain: {} lines were dropped because the log buffer was full",
                self.dropped
            ));
        }
        if self.write_errors > 0 {
            parts.push(format!(
                "log drain: {} lines failed to write",
                self.write_errors
            ));
        }
        (!parts.is_empty()).then(|| parts.join("; "))
    }
}

/// Wait until the writer has finished every line queued so far, or `deadline`.
fn wait_for_drain(deadline: Duration) -> Drain {
    let queued = LINES_QUEUED.load(Ordering::Acquire);
    let started = Instant::now();
    loop {
        let drain = Drain {
            queued,
            done: LINES_DONE.load(Ordering::Acquire),
            dropped: LINES_DROPPED.get().map_or(0, |c| c.dropped_lines() as u64),
            write_errors: LINE_WRITE_ERRORS.load(Ordering::Relaxed),
            waited: started.elapsed(),
        };
        if drain.complete() || drain.waited >= deadline {
            return drain;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Write one line synchronously to stderr (as is, the shape `eprintln!` lines
/// always had there) and, timestamped like the lines around it, to the log
/// (the log file, or stdout when the process logs to stdout), bypassing the
/// buffer.
///
/// Best effort by design: this runs while the process is dying. A failure to
/// write the log file is reported on stderr; a failure to write stderr itself
/// has nowhere left to be reported.
pub fn write_line(message: &str) {
    let _ = writeln!(std::io::stderr(), "{message}"); // last resort; see above
    let line = format!("{} {message}\n", utc_timestamp(SystemTime::now()));
    let Some(sink) = SINK.get() else {
        return; // before tracing started: stderr is the only log
    };
    match &sink.log_file {
        Some(path) => {
            let written = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .and_then(|mut file| file.write_all(line.as_bytes()));
            if let Err(e) = written {
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrosa: could not write the last line to {}: {e}",
                    path.display()
                );
            }
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            if let Err(e) = stdout
                .write_all(line.as_bytes())
                .and_then(|()| stdout.flush())
            {
                let _ = writeln!(
                    std::io::stderr(),
                    "ferrosa: could not write the last line to stdout: {e}"
                );
            }
        }
    }
}

/// `YYYY-MM-DDTHH:MM:SS.ffffffZ`, the shape of the tracing lines around it.
fn utc_timestamp(now: SystemTime) -> String {
    let since = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        since.subsec_micros()
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's
/// `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(queued: u64, done: u64, dropped: u64, write_errors: u64) -> Drain {
        Drain {
            queued,
            done,
            dropped,
            write_errors,
            waited: Duration::from_secs(30),
        }
    }

    /// A drain cut short, a full buffer and failed writes are each reported;
    /// a clean drain reports nothing.
    #[test]
    fn a_short_drain_is_reported_with_its_shortfall() {
        assert_eq!(drain(10, 10, 0, 0).problem(), None);
        assert_eq!(
            drain(20_000, 209, 0, 0).problem().as_deref(),
            Some("log drain incomplete after 30s: 19791 of 20000 queued lines not written")
        );
        assert_eq!(
            drain(10, 7, 3, 1).problem().as_deref(),
            Some(
                "log drain: 3 lines were dropped because the log buffer was full; \
                 log drain: 1 lines failed to write"
            )
        );
    }

    #[test]
    fn timestamps_are_utc_civil_dates() {
        let at = |secs: u64| utc_timestamp(UNIX_EPOCH + std::time::Duration::from_secs(secs));
        assert_eq!(at(0), "1970-01-01T00:00:00.000000Z");
        // 2026-10-10T16:55:00Z, the minute node2 died.
        assert_eq!(at(1_791_651_300), "2026-10-10T16:55:00.000000Z");
        // A leap day.
        assert_eq!(at(951_782_400), "2000-02-29T00:00:00.000000Z");
    }
}
