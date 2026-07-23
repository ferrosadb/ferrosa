//! Module: Detect async-runtime scheduling stalls (a blocked tokio worker).
//! Correctness: Correct when a liveness task that intends to wake every `tick`
//!   records an overrun exactly when its actual gap exceeds `tick + threshold`,
//!   and the stall counters are monotonic.
//! Last revised: 2026-07-22
//! Last changed: New module — Phase 3 (O_DIRECT + I/O). Operationalizes the
//!   2026-07-22 Fly finding: under disk saturation, tokio workers block on I/O
//!   (D-state, `rq_qos_wait`/`folio_wait`) and the whole runtime freezes for
//!   seconds — invisible in production until now (only caught in a gdb run).
//!   This makes that freeze a first-class metric so the coming I/O-pacing /
//!   O_DIRECT fixes can be *verified* in prod, and so a live freeze alerts.
//!
//! # How
//!
//! A cheap task sleeps `tick` (default 100 ms) in a loop and measures the
//! *actual* wall-clock gap between wakes. If the runtime is healthy the gap is
//! ~`tick`; if a worker is blocked (or all are), the timer cannot be serviced on
//! time and the gap balloons. An overrun beyond `threshold` is a scheduling
//! stall — recorded as a counter + summed + max gauge. The task itself is
//! near-zero cost and, being on the runtime, measures exactly what interactive
//! requests experience.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

static RUNTIME_STALL_EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static RUNTIME_STALL_MICROS_TOTAL: AtomicU64 = AtomicU64::new(0);
static RUNTIME_STALL_MAX_MICROS: AtomicU64 = AtomicU64::new(0);

/// Scheduling stalls observed since start (a stall = the liveness task woke
/// `>= threshold` late). Non-zero means a runtime worker was blocked long enough
/// to degrade interactive latency — under disk saturation, this is the freeze.
pub fn runtime_stall_events_total() -> u64 {
    RUNTIME_STALL_EVENTS_TOTAL.load(Ordering::Relaxed)
}

/// Cumulative microseconds of scheduling overrun across all stalls.
pub fn runtime_stall_micros_total() -> u64 {
    RUNTIME_STALL_MICROS_TOTAL.load(Ordering::Relaxed)
}

/// Longest single scheduling stall observed (microseconds).
pub fn runtime_stall_max_micros() -> u64 {
    RUNTIME_STALL_MAX_MICROS.load(Ordering::Relaxed)
}

fn record_stall(overrun: Duration) {
    let micros = overrun.as_micros() as u64;
    RUNTIME_STALL_EVENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    RUNTIME_STALL_MICROS_TOTAL.fetch_add(micros, Ordering::Relaxed);
    RUNTIME_STALL_MAX_MICROS.fetch_max(micros, Ordering::Relaxed);
}

/// The scheduling overrun for a wake whose actual `gap` exceeded the intended
/// `tick`: `Some(gap - tick)` when that overrun is at least `threshold`, else
/// `None`. Pure — the stall decision, testable without a runtime.
pub fn stall_overrun(gap: Duration, tick: Duration, threshold: Duration) -> Option<Duration> {
    let overrun = gap.saturating_sub(tick);
    (overrun >= threshold).then_some(overrun)
}

/// Default liveness cadence.
pub const DEFAULT_TICK: Duration = Duration::from_millis(100);
/// Default stall threshold: a wake this far past its deadline is a real stall
/// (well above timer/scheduling jitter, well below the multi-second freezes).
pub const DEFAULT_THRESHOLD: Duration = Duration::from_millis(300);

/// Spawn the runtime-stall monitor on the current tokio runtime. It wakes every
/// `tick` and, on any wake that is `>= threshold` late, records the stall and
/// invokes `on_stall(overrun)` (the caller logs — this leaf crate stays free of
/// a logging dependency). Call once at boot from within the runtime being
/// watched. The returned [`JoinHandle`] can be dropped (fire-and-forget).
pub fn spawn<F>(tick: Duration, threshold: Duration, on_stall: F) -> tokio::task::JoinHandle<()>
where
    F: Fn(Duration) + Send + 'static,
{
    tokio::spawn(async move {
        let mut last = tokio::time::Instant::now();
        loop {
            tokio::time::sleep(tick).await;
            let now = tokio::time::Instant::now();
            let gap = now.duration_since(last);
            last = now;
            if let Some(overrun) = stall_overrun(gap, tick, threshold) {
                record_stall(overrun);
                on_stall(overrun);
            }
        }
    })
}

/// Render the runtime-stall metrics (text exposition). Concatenated by the web
/// layer alongside the other `ferrosa_sched_*` metrics.
pub fn render_prometheus(out: &mut String) {
    out.push_str(
        "# HELP ferrosa_sched_runtime_stall_events_total Async-runtime scheduling stalls (a tokio worker blocked long enough to delay the liveness task); non-zero degrades interactive latency and should alert.\n\
         # TYPE ferrosa_sched_runtime_stall_events_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_runtime_stall_events_total {}\n",
        runtime_stall_events_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_runtime_stall_micros_total Cumulative microseconds of runtime scheduling overrun across all stalls.\n\
         # TYPE ferrosa_sched_runtime_stall_micros_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_runtime_stall_micros_total {}\n",
        runtime_stall_micros_total()
    ));
    out.push_str(
        "# HELP ferrosa_sched_runtime_stall_max_micros Longest single runtime scheduling stall (microseconds).\n\
         # TYPE ferrosa_sched_runtime_stall_max_micros gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_sched_runtime_stall_max_micros {}\n",
        runtime_stall_max_micros()
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    #[test]
    fn overrun_flags_only_beyond_threshold() {
        let tick = Duration::from_millis(100);
        let thr = Duration::from_millis(300);
        // On-time-ish wake: 110ms gap → 10ms overrun < 300ms → not a stall.
        assert_eq!(stall_overrun(Duration::from_millis(110), tick, thr), None);
        // A 2.5s freeze: gap 2600ms → overrun 2500ms ≥ 300ms → stall.
        assert_eq!(
            stall_overrun(Duration::from_millis(2600), tick, thr),
            Some(Duration::from_millis(2500))
        );
        // Exactly at threshold counts.
        assert_eq!(
            stall_overrun(Duration::from_millis(400), tick, thr),
            Some(Duration::from_millis(300))
        );
        // A gap shorter than the tick never underflows.
        assert_eq!(stall_overrun(Duration::from_millis(50), tick, thr), None);
    }

    #[test]
    fn record_stall_is_monotonic_and_tracks_max() {
        let before_n = runtime_stall_events_total();
        let before_sum = runtime_stall_micros_total();
        record_stall(Duration::from_millis(500));
        record_stall(Duration::from_millis(1200));
        assert!(runtime_stall_events_total() >= before_n + 2);
        assert!(runtime_stall_micros_total() >= before_sum + 1_700_000);
        assert!(runtime_stall_max_micros() >= 1_200_000);
    }

    /// End-to-end: a blocked runtime worker is detected. On a current-thread
    /// runtime the monitor and a blocking `std::thread::sleep` share the one
    /// worker, so the block delays the monitor's wake into a recorded stall.
    #[tokio::test(flavor = "current_thread")]
    async fn detects_a_blocked_runtime_worker() {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let monitor = spawn(
            Duration::from_millis(20),
            Duration::from_millis(150),
            move |_overrun| {
                h.fetch_add(1, Ordering::SeqCst);
            },
        );
        // Let the monitor task start and park on its first tick timer — this
        // establishes its baseline `last`. Without this the spawned task hasn't
        // run yet (current-thread only schedules it when we await), so the block
        // below would precede its baseline and register no stall.
        tokio::time::sleep(Duration::from_millis(5)).await;
        // Block the single runtime thread well past tick+threshold; the monitor's
        // overdue timer cannot be serviced until this returns → a very late wake.
        std::thread::sleep(Duration::from_millis(400));
        // Yield so the runtime can finally service the overdue timer.
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            if hits.load(Ordering::SeqCst) > 0 {
                break;
            }
        }
        monitor.abort();
        assert!(
            hits.load(Ordering::SeqCst) > 0,
            "a >400ms block of the runtime worker must register a stall"
        );
    }
}
