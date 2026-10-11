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
//! ## When the guard is allowed to fire — recent leader contact
//!
//! Suppression only helps if a LIVE leader exists to catch the divergent
//! follower up.  A node cannot ask the cluster whether that is true, and a node
//! with no leader is *always* a `Candidate` with a stalled log — locally
//! identical to a divergent follower a healthy leader is about to repair.  Gating
//! the detectors on `metrics.current_leader.is_some()` therefore silenced them in
//! exactly the case they exist for: a divergent follower isolated from a leader it
//! can no longer hear reports `current_leader == None` too, so both detectors went
//! quiet and the follower stormed on (the 2026-10-11 regression).
//!
//! The two cases differ only in *time since last leader contact*.  So the guard
//! tracks when it last observed a leader and suppresses only while that is within
//! `LEADER_CONTACT_WINDOW_MS`.  In a leaderless outage no node has seen a leader
//! for far longer than the window, so all suppression lapses and the cluster
//! re-elects; a divergent follower was following its leader moments ago, so it is
//! still suppressed and the leader catches it up.  The window starts at the guard's
//! own start too, because the guard is spawned at node bootstrap — a freshly
//! (re)joined node is given one window of grace before the guard gives up on it.
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

/// How long an observed leader keeps the guard willing to suppress (ms).
///
/// Suppression exists so a LIVE leader can catch a divergent follower up, so it
/// is justified only while the node has had leader contact recently.  A node
/// cannot ask the cluster whether a leader exists — and a leaderless node is
/// *always* a `Candidate` with a stalled log, locally indistinguishable from a
/// divergent follower a healthy leader is about to repair.  The two cases differ
/// only in the *time since last leader contact*: a divergent follower was
/// following its leader seconds ago, whereas in a leaderless outage no node has
/// seen a leader for far longer.  The guard therefore suppresses only while it
/// has observed `current_leader.is_some()` within this window.
///
/// Bound — why `STORM_SUPPRESS_MS`:
///
///  * Too short and the guard is disabled in the very case it was written for.
///    A full production-cadence detection cycle is `ROLLING_WINDOW_MS` (30 s,
///    for the window to become all-`Candidate`) plus the two rejected elections
///    the detector needs (`>= 2 * election_timeout_max`, ~12 s at the 6 s fleet
///    maximum) ≈ 42 s.  The window must clear 42 s, so the existing
///    `ROLLING_WINDOW_MS` (30 s) is too tight by construction.
///  * Too long and it re-opens the outage.  The 2026-10-11 leaderless stretch
///    was ~35 min; 60 s is ~35× shorter, so a node that loses its leader always
///    stops suppressing long before an outage could repeat. `STORM_SUPPRESS_MS`
///    (60 s) clears 42 s with 1.4× margin and is already the guard's suppression
///    quantum, so a divergent follower gets exactly one suppression window of
///    grace after the last leader contact and no more.
const LEADER_CONTACT_WINDOW_MS: u64 = STORM_SUPPRESS_MS;

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
// Pure detection decisions
// ---------------------------------------------------------------------------
//
// Extracted from the poll loop so the guard's decisions are testable without a
// live `Raft` instance.  All are pure: same inputs, same answer, no I/O.
//
// # Invariants
//
//   1. A node that has had no leader contact within `LEADER_CONTACT_WINDOW_MS`
//      is never muted, so a genuinely leaderless cluster always regains a
//      willing candidate within one window and the 2026-10-11 outage cannot
//      recur.
//   2. A divergent follower whose log is stale, on a cluster with a live leader
//      (so it saw that leader within the window), IS muted so the leader can
//      catch it up.
//   3. Neither depends on global cluster state: a node only ever consults its
//      own `current_leader` observations and its own clock.
//
// Both detectors gate on `leader_recently`, which is
// [`leader_contact_is_recent`] evaluated over this node's last leader
// observation.  Gating on `metrics.current_leader.is_some()` directly was the
// bug: it is `None` both in a leaderless outage AND on a divergent follower that
// is isolated from a live leader, so it silenced the detector in the very case
// it exists for.

/// Whether a leader was observed recently enough that suppressing elections is
/// still justified — i.e. a live leader was seen within `window` and can catch a
/// divergent follower up.  Pure and lock-free; `saturating_duration_since` keeps
/// a clock that stepped backwards from panicking.
pub(crate) fn leader_contact_is_recent(
    now: Instant,
    last_leader_seen: Instant,
    window: Duration,
) -> bool {
    now.saturating_duration_since(last_leader_seen) < window
}

/// Fast-path burst detector (P0-17): two or more elections fired and were
/// rejected inside one poll window while this node was a `Candidate`, with no
/// log progress, and a leader was seen recently enough to repair the divergence.
pub(crate) fn burst_storm(
    state: ServerState,
    term_delta: u64,
    log_advanced: bool,
    leader_recently: bool,
) -> bool {
    leader_recently
        && state == ServerState::Candidate
        && term_delta > TERM_JUMP_THRESHOLD
        && !log_advanced
}

/// Slow-path rolling-window detector (P0-19): every sample in the window was a
/// `Candidate`, at least [`ROLLING_WINDOW_MIN_JUMPS`] elections fired, the log
/// did not advance, and a leader was seen recently enough to repair the
/// divergence.
pub(crate) fn rolling_window_storm(
    all_candidate: bool,
    total_jumps: u64,
    log_stalled: bool,
    leader_recently: bool,
) -> bool {
    leader_recently && all_candidate && total_jumps >= ROLLING_WINDOW_MIN_JUMPS && log_stalled
}

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

    // Leader-contact epoch.  Seeded to "now" because the guard is spawned at node
    // bootstrap (`controller::cluster`), so a node that has *just* come up is
    // presumed to be joining a cluster that still has (or is about to elect) a
    // leader.  Refreshed on every poll that observes a leader; once it ages past
    // LEADER_CONTACT_WINDOW_MS the guard stops suppressing, so a genuinely
    // leaderless cluster always regains a willing candidate within one window.
    let mut last_leader_seen: Instant = Instant::now();

    let poll = Duration::from_millis(POLL_INTERVAL_MS);
    let rolling_window = Duration::from_millis(ROLLING_WINDOW_MS);
    let leader_window = Duration::from_millis(LEADER_CONTACT_WINDOW_MS);

    // Rolling window buffer — oldest samples at front, newest at back.
    let mut window: VecDeque<Sample> = VecDeque::new();

    tracing::debug!(
        election_timeout_min_ms,
        poll_interval_ms = POLL_INTERVAL_MS,
        term_jump_threshold = TERM_JUMP_THRESHOLD,
        rolling_window_ms = ROLLING_WINDOW_MS,
        rolling_window_min_jumps = ROLLING_WINDOW_MIN_JUMPS,
        leader_contact_window_ms = LEADER_CONTACT_WINDOW_MS,
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
        // Refresh the leader-contact epoch whenever a leader is current, then ask
        // whether suppression is still justified.  A storm can only be ridden out
        // by a LIVE leader catching a divergent follower up; a leaderless node is
        // *always* a `Candidate` with a stalled log, so a bare `current_leader`
        // test cannot tell "the cluster has no leader" (never suppress) from "this
        // node is isolated from a live leader it saw moments ago" (suppress).  The
        // age of the last leader observation is the only local signal that does.
        if metrics.current_leader.is_some() {
            last_leader_seen = now;
        }
        let leader_recently = leader_contact_is_recent(now, last_leader_seen, leader_window);

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
        let burst_storm = burst_storm(state, term_delta, log_advancing, leader_recently);

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
                rolling_window_storm(all_candidate, total_jumps, log_stalled, leader_recently);

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact signature seen on every node during the 2026-10-11 outage: a
    /// leaderless `Candidate` re-electing itself with no log progress. Both
    /// detectors must fire for it while a leader exists — the guard's stated
    /// purpose (let that leader catch the divergent follower up).
    #[test]
    fn both_detectors_fire_on_the_observed_storm_shape_while_a_leader_lives() {
        assert!(
            burst_storm(ServerState::Candidate, 5, false, true),
            "burst: term jumped by 5 with no log progress"
        );
        assert!(
            rolling_window_storm(true, 2, true, true),
            "rolling-window: candidate for the whole window, 2 jumps, log stalled"
        );
    }

    // --- Invariant: a leaderless cluster is never muted ---------------------

    /// THE DEFECT. A node with no leader is *always* a `Candidate`; when the
    /// cluster is leaderless every node sees this same shape at the same
    /// instant. Muting all of them leaves nobody willing to call an election,
    /// so the guard holds open the very outage it exists to shorten.
    #[test]
    fn neither_detector_mutes_elections_when_the_cluster_has_no_leader() {
        assert!(
            !burst_storm(ServerState::Candidate, 5, false, false),
            "burst detector must not suppress elections with no leader to catch up from"
        );
        assert!(
            !rolling_window_storm(true, 2, true, false),
            "rolling-window detector must not suppress elections with no leader to catch up from"
        );
    }

    /// Every storm shape either detector recognises, driven with
    /// `has_leader = false`. Each must stay silent: a detector that fires here
    /// is a detector that can only ever make a leaderless cluster worse.
    #[test]
    fn no_storm_shape_suppresses_elections_in_a_leaderless_cluster() {
        let shapes = [
            (ServerState::Candidate, 0u64, false),
            (ServerState::Candidate, 1, false),
            (ServerState::Candidate, 2, false),
            (ServerState::Candidate, 99, false),
            (ServerState::Candidate, 2, true),
            (ServerState::Follower, 2, false),
            (ServerState::Leader, 2, false),
            (ServerState::Learner, 2, false),
            (ServerState::Shutdown, 2, false),
        ];
        for (state, delta, advanced) in shapes {
            assert!(
                !burst_storm(state, delta, advanced, false),
                "burst detector fired with no leader (state={state:?} delta={delta} advanced={advanced})"
            );
        }
        for all_candidate in [false, true] {
            for jumps in [0u64, 1, 2, 99] {
                for stalled in [false, true] {
                    assert!(
                        !rolling_window_storm(all_candidate, jumps, stalled, false),
                        "rolling-window detector fired with no leader \
                         (all_candidate={all_candidate} jumps={jumps} stalled={stalled})"
                    );
                }
            }
        }
    }

    // --- The guard must still be a guard -----------------------------------

    /// A single rejected election is a legitimate election, not a storm — the
    /// gate must not have disabled the detectors outright.
    #[test]
    fn a_single_rejected_election_is_not_a_storm() {
        assert!(
            !burst_storm(ServerState::Candidate, 1, false, true),
            "delta of 1 is one election, below TERM_JUMP_THRESHOLD"
        );
        assert!(
            !rolling_window_storm(true, 1, true, true),
            "one jump is below ROLLING_WINDOW_MIN_JUMPS"
        );
    }

    /// Log progress is never a storm, and a node that is not a `Candidate` is
    /// not storming either.
    #[test]
    fn log_progress_or_a_settled_state_is_not_a_storm() {
        assert!(
            !burst_storm(ServerState::Candidate, 3, true, true),
            "an advancing log is catch-up, not a storm"
        );
        for state in [
            ServerState::Follower,
            ServerState::Leader,
            ServerState::Learner,
            ServerState::Shutdown,
        ] {
            assert!(
                !burst_storm(state, 9, false, true),
                "{state:?} is not a storming candidate"
            );
        }
        assert!(
            !rolling_window_storm(true, 9, false, true),
            "an advancing log is catch-up, not a storm"
        );
        assert!(
            !rolling_window_storm(false, 9, true, true),
            "one non-candidate sample means the window is not all-candidate"
        );
    }

    /// The slow-path threshold is inclusive at exactly `ROLLING_WINDOW_MIN_JUMPS`.
    #[test]
    fn rolling_window_threshold_is_inclusive_at_two_jumps() {
        assert!(rolling_window_storm(true, 2, true, true));
        assert!(!rolling_window_storm(true, 1, true, true));
    }

    // --- Leader-contact window: the signal that separates (A) from (B) -------

    /// The epoch predicate is what distinguishes the two locally-identical
    /// `Candidate`-with-stalled-log states:
    ///
    ///  * (B) divergent follower — a live leader was seen seconds ago → recent →
    ///    suppression is allowed so that leader can catch the follower up; and
    ///  * (A) leaderless outage — no leader seen for far longer than the window →
    ///    not recent → suppression lapses and the cluster can re-elect.
    ///
    /// Asserted on values (the boolean), not on counts.
    #[test]
    fn suppression_is_allowed_only_within_the_leader_contact_window() {
        let window = Duration::from_millis(LEADER_CONTACT_WINDOW_MS);
        let t0 = Instant::now();

        // (B) leader seen 3 s ago — a divergent follower the leader is repairing.
        assert!(
            leader_contact_is_recent(t0 + Duration::from_secs(3), t0, window),
            "contact 3 s ago must keep suppression allowed so the leader can catch up"
        );
        // (B) at production cadence the last contact can be ~30 s old by the time
        // the rolling window fires; that must still be allowed.
        assert!(
            leader_contact_is_recent(t0 + Duration::from_secs(30), t0, window),
            "contact 30 s ago is within the window and must still suppress"
        );
        // (A) leaderless outage — no leader for 10 min, far past the window.
        assert!(
            !leader_contact_is_recent(t0 + Duration::from_secs(600), t0, window),
            "a node leaderless for minutes must stop suppressing so the cluster re-elects"
        );
        // The window is exclusive at exactly one window.
        assert!(!leader_contact_is_recent(t0 + window, t0, window));
        assert!(leader_contact_is_recent(
            t0 + window - Duration::from_millis(1),
            t0,
            window
        ));
    }

    /// The window must clear one full production-cadence detection cycle
    /// (`ROLLING_WINDOW_MS` to become all-`Candidate` plus two rejected elections),
    /// or the guard dies in the very case it exists for — and it must stay far
    /// below the ~35 min leaderless stretch of the 2026-10-11 outage, or it
    /// re-opens it.  Pinned so a future edit that moves the bound fails loudly.
    #[test]
    fn leader_contact_window_spans_a_detection_cycle_but_not_the_outage() {
        let window = Duration::from_millis(LEADER_CONTACT_WINDOW_MS);
        // 30 s of rolling window + two rejected elections at the 6 s fleet
        // maximum = 42 s.
        let detection_cycle = Duration::from_millis(ROLLING_WINDOW_MS) + Duration::from_secs(2 * 6);
        assert!(
            window > detection_cycle,
            "leader-contact window ({window:?}) must exceed one detection cycle \
             ({detection_cycle:?}) or the guard is disabled in the divergent-follower case"
        );
        // The window must stay far shorter than the ~35 min leaderless outage.
        let outage = Duration::from_secs(35 * 60);
        assert!(
            window * 10 < outage,
            "leader-contact window ({window:?}) must stay far shorter than the ~35 min outage"
        );
    }
}
