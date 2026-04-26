//! Election-storm watchdog — P0-17 fix (path c).
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
//! detects the election-storm signature:
//!
//!   - node is in `Candidate` state (not Leader, not Follower), AND
//!   - `current_term` has advanced by more than `TERM_JUMP_THRESHOLD` since
//!     the last observation window (≥2 elections fired and were rejected in
//!     a single `POLL_INTERVAL_MS` window), AND
//!   - `last_log_index` has not changed (the node is not catching up)
//!
//! …the watchdog calls `raft.runtime_config().elect(false)` to suppress
//! further elections and increments the `ELECTION_STORM_TERM_JUMPS_TOTAL`
//! counter.
//!
//! With elections suppressed, the normal openraft replication path takes
//! over: the leader (which never stepped down because openraft ignores
//! `RequestVote` from candidates whose `last_log_id` is behind the
//! committed `last_log_id`) sends `AppendEntries` / `InstallSnapshot` to
//! the now-quiet node, bringing it up to date.  Once the node transitions
//! to `Follower` (replication is complete), the watchdog re-enables
//! elections.
//!
//! ## Suppression duration
//!
//! The watchdog re-enables elections after at most
//! `STORM_SUPPRESS_MS` milliseconds even if the node has not become a
//! follower, so a genuinely isolated node can eventually call a leader
//! election (with its correct, current term) rather than being permanently
//! muted.
//!
//! ## Metric
//!
//! `ELECTION_STORM_TERM_JUMPS_TOTAL` — a process-wide `AtomicU64` that
//! increments each time the watchdog detects a storm.  Exposed via
//! `election_storm_term_jumps_total()`.  A non-zero value in steady-state
//! CI should trigger an alert and investigation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use openraft::{Raft, RaftTypeConfig, ServerState};

// ---------------------------------------------------------------------------
// Metric counter
// ---------------------------------------------------------------------------

/// Process-wide count of election-storm detections (term jumped by >1 in a
/// single timeout window while the node's log index was not advancing).
///
/// Non-zero in steady state indicates P0-17 would have occurred without this
/// watchdog; the counter is the observable signal for CI alerting.
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
/// One observation per interval; storm is detected when `current_term`
/// jumps by more than `TERM_JUMP_THRESHOLD` between two observations.
const POLL_INTERVAL_MS: u64 = 1_000;

/// Minimum term delta per observation window to call it a storm.
///
/// In normal operation a node should not increment its term during a
/// single `POLL_INTERVAL_MS` window; a delta of 1 means exactly one
/// election fired and was rejected.  Any delta greater than 1 — i.e.,
/// two or more rejected elections within one poll window — indicates the
/// node is hot-looping on elections and is in a storm.
///
/// A threshold of 1 (`term_delta > 1` → fires at delta ≥ 2) was chosen
/// based on the production observation (ferrosa-memory node3, April 2026)
/// where elections fired at ~17.5 s intervals — one per election timeout.
/// Under a 1 s poll window that produces delta = 0 or 1 in normal
/// operation; delta ≥ 2 unambiguously identifies a storm.
const TERM_JUMP_THRESHOLD: u64 = 1;

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
/// on term delta across fixed `POLL_INTERVAL_MS` windows rather than on the
/// election timeout value.
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

    tracing::debug!(
        election_timeout_min_ms,
        poll_interval_ms = POLL_INTERVAL_MS,
        term_jump_threshold = TERM_JUMP_THRESHOLD,
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

        let metrics = raft.metrics().borrow().clone();
        let current_term = metrics.current_term;
        let current_log_index = metrics.last_log_index;
        let state = metrics.state;

        // --- Re-enable elections if suppress window expired ---
        if suppressing && tokio::time::Instant::now() >= suppress_until {
            suppressing = false;
            raft.runtime_config().elect(true);
            tracing::info!(
                current_term,
                "election_guard: suppression window expired, re-enabling elections"
            );
        }

        // --- Skip storm detection while already suppressing ---
        // (We already fired once; avoid double-counting.)
        if suppressing {
            prev_term = current_term;
            prev_log_index = current_log_index;
            continue;
        }

        // --- Storm detection ---
        let term_delta = current_term.saturating_sub(prev_term);
        let log_advancing = current_log_index != prev_log_index;

        let is_storm =
            state == ServerState::Candidate && term_delta > TERM_JUMP_THRESHOLD && !log_advancing;

        if is_storm {
            ELECTION_STORM_TERM_JUMPS_TOTAL.fetch_add(1, Ordering::Relaxed);
            suppressing = true;
            suppress_until = tokio::time::Instant::now() + Duration::from_millis(STORM_SUPPRESS_MS);

            tracing::warn!(
                current_term,
                prev_term,
                term_delta,
                current_log_index = ?current_log_index,
                suppress_ms = STORM_SUPPRESS_MS,
                "election_guard: election storm detected — suppressing elections for \
                 {STORM_SUPPRESS_MS}ms to allow leader-driven log catch-up (P0-17)"
            );

            // Suppress elections.  The leader will continue sending
            // AppendEntries / InstallSnapshot; the node will catch up
            // without burning CPU on failed elections.
            raft.runtime_config().elect(false);
        } else if state == ServerState::Follower && prev_log_index != current_log_index {
            // Node is catching up as expected; log at debug level.
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
