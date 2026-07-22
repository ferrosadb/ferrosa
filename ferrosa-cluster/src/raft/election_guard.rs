//! Election-storm watchdog — P0-17 fix (path c), P0-19 cadence fix.
//!
//! # Deprecation status (Sprint 4)
//!
//! Per ADR-012, this module is scheduled for deletion in W4.11 once
//! the bolt-on retirement gate fires green (see
//! [`crate::controller::bootstrap::retirement_gate`]).  The gate
//! requires a 2-week clean Jepsen window against the Sprint 3
//! PreVote+CheckQuorum build.  Until the manifest at
//! `specs/in-process/sprint-04-jepsen-window.json` is populated and
//! reports a clean run set, this module continues to provide the
//! safety net.  **Do not add new dependencies on it.**
//!
//! ## Problem
//!
//! When a Raft follower's log diverges from the cluster (e.g. after a
//! prolonged network partition), it repeatedly times out, bumps its term,
//! sends `RequestVote`, and is rejected with `seen a greater log id`.  Its
//! local term increments unbounded while the cluster term stays stable.
//! Observed: ~18 000 failed elections over 32 hours on the live
//! `ferrosa-memory` cluster (node3, April 2026).
//!
//! ## Root cause
//!
//! openraft 0.9.x does **not** implement Raft pre-vote (Ongaro §9.6).
//! There is no hook to intercept a candidate's internal handling of a
//! `VoteResponse` that includes a `greater_log_id`.  The candidate
//! unconditionally retries the election.
//!
//! ## Fix (path c — divergence-detection backoff)
//!
//! A background watchdog task monitors the local Raft metrics.  When it
//! detects the election-storm signature it calls
//! `raft.runtime_config().elect(false)` for `STORM_SUPPRESS_MS` (60 s)
//! and increments `ELECTION_STORM_TERM_JUMPS_TOTAL`.
//!
//! Two independent detectors run on every poll cycle:
//!
//! ### Fast-path: burst detector (P0-17, original)
//!
//! Fires when `current_term` advances by more than `TERM_JUMP_THRESHOLD`
//! (> 1) within a single `POLL_INTERVAL_MS` (1 s) window.  Catches storms
//! where election_timeout_min is short (≤ 500 ms), i.e., multiple elections
//! per second.
//!
//! ### Slow-path: rolling-window detector (P0-19)
//!
//! Fires when all of the following hold over a rolling `ROLLING_WINDOW_MS`
//! (30 s) window:
//!
//!   - `state == Candidate` for the entire window (every sample)
//!   - `total_term_jumps_in_window >= 2` (at least two separate elections
//!     fired and were rejected)
//!   - `log_index` has not advanced since the start of the window
//!
//! This catches production-cadence storms (election_timeout_min = 3000 ms)
//! where per-poll delta is 0 or 1 and never exceeds `TERM_JUMP_THRESHOLD`.
//! Two failed elections without log progress over 30 s is unambiguous storm
//! regardless of within-poll delta.
//!
//! ## Suppression duration
//!
//! Elections are re-enabled after at most `STORM_SUPPRESS_MS` milliseconds
//! even if the node has not become a follower, so a genuinely isolated node
//! can eventually call a leader election rather than being permanently muted.
//!
//! ## Metric
//!
//! `ELECTION_STORM_TERM_JUMPS_TOTAL` — a process-wide `AtomicU64` that
//! increments each time the watchdog detects a storm (one increment per
//! detection event, not per per-poll observation).  Exposed via
//! `election_storm_term_jumps_total()`.  A non-zero value in steady-state
//! CI should trigger an alert and investigation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use openraft::{Raft, RaftTypeConfig, ServerState};

// ---------------------------------------------------------------------------
// Metric counter
// ---------------------------------------------------------------------------

/// Process-wide count of election-storm detections.
///
/// Increments once per storm detection event (not per per-poll observation).
/// Non-zero in steady state indicates P0-17/P0-19 would have occurred without
/// this watchdog; the counter is the observable signal for CI alerting.
pub static ELECTION_STORM_TERM_JUMPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Return the current value of the election-storm term-jump counter.
///
/// Intended for tests and the metrics endpoint.  Thread-safe; uses
/// `Relaxed` ordering (monotonically increasing counter, no ordering
/// guarantee needed).
pub fn election_storm_term_jumps_total() -> u64 {
    ELECTION_STORM_TERM_JUMPS_TOTAL.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Watchdog parameters
// ---------------------------------------------------------------------------

/// How long to suppress elections after a storm is detected (milliseconds).
///
/// 60 s gives the leader comfortably more than one snapshot-install
/// round-trip, even under high load.  After this window, elections are
/// re-enabled regardless of whether the node has become a follower.
const STORM_SUPPRESS_MS: u64 = 60_000;

/// Watchdog poll interval (milliseconds).
///
/// One observation per interval; burst storm is detected when `current_term`
/// jumps by more than `TERM_JUMP_THRESHOLD` between two observations.
const POLL_INTERVAL_MS: u64 = 1_000;

/// Minimum term delta per 1-second observation window to call it a burst storm.
///
/// A delta of 1 means exactly one election fired and was rejected.  Any delta
/// greater than 1 — i.e., two or more rejected elections within one poll
/// window — indicates a fast-cadence storm.
///
/// A threshold of 1 (`term_delta > 1` → fires at delta ≥ 2) covers burst
/// scenarios (election_timeout_min ≤ ~500 ms).
const TERM_JUMP_THRESHOLD: u64 = 1;

/// Rolling window length for the slow-path production-cadence detector.
///
/// 30 s covers at least 2 election cycles at production cadence
/// (election_timeout_min = 3000 ms → ~6–17 s/election in practice).
/// Two failed elections without log progress in this window is unambiguous
/// storm signal regardless of per-poll delta.
const ROLLING_WINDOW_MS: u64 = 30_000;

/// Minimum total term jumps across the rolling window to fire the slow
/// detector.  Two is the smallest count that rules out a single legitimate
/// election (e.g. on cluster startup).
const ROLLING_WINDOW_MIN_JUMPS: u64 = 2;

// ---------------------------------------------------------------------------
// Rolling-window sample
// ---------------------------------------------------------------------------

/// One observation captured by the watchdog poll loop.
#[derive(Clone, Debug)]
struct Sample {
    /// Wall-clock time of observation.
    at: Instant,
    /// Raft term at observation time.
    term: u64,
    /// Last log index at observation time (`None` means empty log).
    log_index: Option<u64>,
    /// Node state at observation time.
    state: ServerState,
    /// Term delta since the previous sample (saturating).
    term_delta: u64,
}

// ---------------------------------------------------------------------------
// Watchdog task
// ---------------------------------------------------------------------------

/// Spawn the election-storm watchdog for a Raft node.
///
/// The task runs indefinitely (or until `cancel` is triggered) and monitors
/// the given `raft` instance for the storm signature described in the module
/// doc.  Call once per node after the Raft instance is created.
///
/// `election_timeout_min_ms` is used only for logging; the watchdog relies
/// on term delta across fixed windows rather than on the election timeout
/// value.
pub async fn run_election_guard<C>(
    raft: Arc<Raft<C>>,
    cancel: tokio_util::sync::CancellationToken,
    election_timeout_min_ms: u64,
) where
    C: RaftTypeConfig<NodeId = u64>,
{
    // Seed baseline from the current metrics so that the first poll window
    // measures delta FROM NOW, not from a zero/None sentinel that would
    // misidentify a stable log as "advancing" (prev=None vs current=Some(n)).
    let seed = raft.metrics().borrow().clone();
    let mut prev_term: u64 = seed.current_term;
    let mut prev_log_index: Option<u64> = seed.last_log_index;
    drop(seed);

    let mut suppressing = false;
    let mut suppress_until = tokio::time::Instant::now();

    let poll = Duration::from_millis(POLL_INTERVAL_MS);
    let rolling_window = Duration::from_millis(ROLLING_WINDOW_MS);

    // Rolling window buffer — oldest samples at front, newest at back.
    let mut window: VecDeque<Sample> = VecDeque::new();

    tracing::debug!(
        election_timeout_min_ms,
        poll_interval_ms = POLL_INTERVAL_MS,
        term_jump_threshold = TERM_JUMP_THRESHOLD,
        rolling_window_ms = ROLLING_WINDOW_MS,
        rolling_window_min_jumps = ROLLING_WINDOW_MIN_JUMPS,
        suppress_ms = STORM_SUPPRESS_MS,
        "election_guard: watchdog started"
    );

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::debug!("election_guard: cancelled, exiting");
                // Re-enable elections in case we were suppressing when cancelled.
                raft.runtime_config().elect(true);
                return;
            }
            _ = tokio::time::sleep(poll) => {}
        }

        let now = Instant::now();
        let metrics = raft.metrics().borrow().clone();
        let current_term = metrics.current_term;
        let current_log_index = metrics.last_log_index;
        let state = metrics.state;

        // Publish the term + leadership sample to the Prometheus surface. This
        // is the guard's role as the process's de-facto raft-metrics poller;
        // per ADR-012 the metric *home* is `consensus_metrics`, not this guard,
        // so retiring the guard only relocates this one publish call.
        super::consensus_metrics::record_raft_state(current_term, state == ServerState::Leader);

        // --- Re-enable elections if suppress window expired ---
        if suppressing && tokio::time::Instant::now() >= suppress_until {
            suppressing = false;
            raft.runtime_config().elect(true);
            tracing::info!(
                current_term,
                "election_guard: suppression window expired, re-enabling elections"
            );
        }

        // --- Skip detection while already suppressing ---
        // (We already fired once; avoid double-counting.  Still update prev_*
        // and push a window sample so the window doesn't accumulate stale data.)
        let term_delta = current_term.saturating_sub(prev_term);

        if suppressing {
            prev_term = current_term;
            prev_log_index = current_log_index;
            // Drain old samples from the window during suppression so that a
            // new storm after the suppression window lifts is detected fresh.
            window.clear();
            continue;
        }

        // --- Fast-path: burst detector (P0-17) ---
        //
        // Two or more elections fired and were rejected within a single 1-second
        // poll window.  Catches short election_timeout_min scenarios.
        let log_advancing = current_log_index != prev_log_index;
        let burst_storm =
            state == ServerState::Candidate && term_delta > TERM_JUMP_THRESHOLD && !log_advancing;

        if burst_storm {
            fire_suppression(
                &raft,
                &mut suppressing,
                &mut suppress_until,
                current_term,
                prev_term,
                term_delta,
                current_log_index,
                "burst",
            );
            prev_term = current_term;
            prev_log_index = current_log_index;
            window.clear();
            continue;
        }

        // --- Slow-path: rolling-window detector (P0-19) ---
        //
        // Push a sample into the rolling window and evict samples older than
        // ROLLING_WINDOW_MS.  Then check the window for the production-cadence
        // storm signature.
        let sample = Sample {
            at: now,
            term: current_term,
            log_index: current_log_index,
            state,
            term_delta,
        };
        window.push_back(sample);

        // Evict samples that have aged out of the rolling window.
        while window
            .front()
            .map(|s| now.duration_since(s.at) > rolling_window)
            .unwrap_or(false)
        {
            window.pop_front();
        }

        // Require at least enough samples to span two election cycles before
        // the slow path can fire.  With POLL_INTERVAL_MS=1s and
        // election_timeout_min≥200ms this is at least 2 samples, which is a
        // cheap guard against false-positives at startup.
        if window.len() >= 2 {
            let window_start = &window[0];
            let window_end = window.back().expect("len >= 2");

            // The storm signature over the rolling window:
            //   1. Every sample in the window shows Candidate state.
            //   2. Total term jumps across the window >= ROLLING_WINDOW_MIN_JUMPS.
            //   3. Log index has not advanced since the window started.
            let all_candidate = window.iter().all(|s| s.state == ServerState::Candidate);
            let total_jumps: u64 = window.iter().map(|s| s.term_delta).sum();
            let log_stalled = window_end.log_index == window_start.log_index;

            let slow_storm =
                all_candidate && total_jumps >= ROLLING_WINDOW_MIN_JUMPS && log_stalled;

            if slow_storm {
                fire_suppression(
                    &raft,
                    &mut suppressing,
                    &mut suppress_until,
                    current_term,
                    window_start.term,
                    total_jumps,
                    current_log_index,
                    "rolling-window",
                );
                window.clear();
            }
        }

        // --- Healthy catch-up logging ---
        if state == ServerState::Follower && prev_log_index != current_log_index {
            tracing::debug!(
                current_term,
                current_log_index = ?current_log_index,
                "election_guard: follower log advancing (healthy catch-up)"
            );
        }

        prev_term = current_term;
        prev_log_index = current_log_index;
    }
}

// ---------------------------------------------------------------------------
// Helper: trigger suppression and emit the warning log
// ---------------------------------------------------------------------------

/// Activate the suppression window and emit the warning log.
///
/// `detector` names the detection path ("burst" or "rolling-window") for
/// the log message.
// Eight parameters stay below the Clippy 8-arg default; the allow here is
// a safety net in case the project's limit is set lower.
#[allow(clippy::too_many_arguments)]
fn fire_suppression<C>(
    raft: &Arc<Raft<C>>,
    suppressing: &mut bool,
    suppress_until: &mut tokio::time::Instant,
    current_term: u64,
    prev_term: u64,
    term_delta: u64,
    current_log_index: Option<u64>,
    detector: &str,
) where
    C: RaftTypeConfig<NodeId = u64>,
{
    ELECTION_STORM_TERM_JUMPS_TOTAL.fetch_add(1, Ordering::Relaxed);
    *suppressing = true;
    *suppress_until = tokio::time::Instant::now() + Duration::from_millis(STORM_SUPPRESS_MS);

    tracing::warn!(
        current_term,
        prev_term,
        term_delta,
        current_log_index = ?current_log_index,
        detector,
        suppress_ms = STORM_SUPPRESS_MS,
        "election_guard: election storm detected ({detector}) — suppressing elections for \
         {STORM_SUPPRESS_MS}ms to allow leader-driven log catch-up (P0-17/P0-19)"
    );

    raft.runtime_config().elect(false);
}
