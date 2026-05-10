//! CheckQuorum decision logic (Ongaro §6.4) — ferrosa fork extension per ADR-012.
//!
//! Pure-function module so the protocol decision is unit-testable in isolation
//! from the engine async state machine. The wire-up into `Notify::Tick` lives
//! in `core::raft_core::handle_tick_check_quorum`, which calls
//! [`CheckQuorum::should_step_down`] each heartbeat.
//!
//! # Protocol
//!
//! Every `heartbeat_interval`, the leader checks whether it has received an
//! `AppendEntries` ack from a quorum of voters within the last
//! `election_timeout_max × check_quorum_ratio` window. If not, it
//! voluntarily transitions to Follower and surrenders its lease.
//!
//! # Why it matters
//!
//! Without CheckQuorum, a leader that has lost majority connectivity does not
//! step down. It remains "leader" indefinitely from its own perspective,
//! refusing to commit any new log entries (because they cannot reach a
//! majority) but also blocking any other node from being elected (because
//! followers that can still reach this leader will keep granting it their
//! vote-rejections, while the rest of the cluster cannot reach quorum
//! either). Clients submitting writes hang. CheckQuorum bounds this window
//! to `election_timeout × ratio`.
//!
//! See `bug-raft-stale-candidate-runaway-term-no-prevote.md` for the
//! companion failure mode.

use std::time::Duration;

/// Decision produced by [`CheckQuorum::should_step_down`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckQuorumDecision {
    /// CheckQuorum is disabled (`check_quorum_ratio == 0.0`). No action.
    Disabled,
    /// Leader is healthy: a quorum acked within the window. No action.
    Healthy,
    /// Leader has not seen a quorum ack within the deadline. **Step down.**
    StepDown {
        /// How long the leader has been without a quorum ack.
        elapsed: Duration,
        /// The deadline window that was exceeded.
        deadline: Duration,
    },
    /// Leader has never seen a quorum ack since becoming leader. This is the
    /// initial state and is treated as healthy for one heartbeat to allow the
    /// first round of acks to arrive. After that, it becomes `StepDown`.
    NeverAcked {
        /// How long the leader has been a leader without any ack.
        elapsed_since_election: Duration,
        deadline: Duration,
    },
}

impl CheckQuorumDecision {
    /// Whether the leader must step down.
    pub(crate) fn must_step_down(&self) -> bool {
        matches!(self, CheckQuorumDecision::StepDown { .. } | CheckQuorumDecision::NeverAcked { .. })
            && match self {
                // NeverAcked transitions to step-down once the elapsed window exceeds the deadline.
                CheckQuorumDecision::NeverAcked {
                    elapsed_since_election,
                    deadline,
                } => elapsed_since_election > deadline,
                CheckQuorumDecision::StepDown { .. } => true,
                _ => false,
            }
    }
}

/// CheckQuorum decision input. All fields are facts the leader already
/// tracks (see `core::raft_core::last_quorum_acked_time`); this struct
/// makes the decision pure and unit-testable.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CheckQuorum {
    /// `Config::check_quorum_ratio`. `0.0` means disabled.
    pub(crate) ratio: f64,
    /// `Config::election_timeout_max` in milliseconds.
    pub(crate) election_timeout_max_ms: u64,
}

impl CheckQuorum {
    /// Evaluate the CheckQuorum step-down condition.
    ///
    /// - `elapsed_since_quorum_ack`: time since the leader last saw an
    ///   AppendEntries ack from a majority of voters. `None` means the leader
    ///   has not seen any ack since taking office.
    /// - `elapsed_since_election`: time since this node became leader.
    ///   Used to grant a grace period for `NeverAcked` startup.
    pub(crate) fn should_step_down(
        &self,
        elapsed_since_quorum_ack: Option<Duration>,
        elapsed_since_election: Duration,
    ) -> CheckQuorumDecision {
        if self.ratio <= 0.0 {
            return CheckQuorumDecision::Disabled;
        }

        let deadline = Duration::from_millis((self.election_timeout_max_ms as f64 * self.ratio) as u64);

        match elapsed_since_quorum_ack {
            None => CheckQuorumDecision::NeverAcked {
                elapsed_since_election,
                deadline,
            },
            Some(elapsed) if elapsed > deadline => CheckQuorumDecision::StepDown { elapsed, deadline },
            Some(_) => CheckQuorumDecision::Healthy,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cq(ratio: f64) -> CheckQuorum {
        CheckQuorum {
            ratio,
            election_timeout_max_ms: 1000,
        }
    }

    #[test]
    fn disabled_when_ratio_zero() {
        let decision = cq(0.0).should_step_down(Some(Duration::from_secs(60)), Duration::from_secs(60));
        assert_eq!(decision, CheckQuorumDecision::Disabled);
        assert!(!decision.must_step_down());
    }

    #[test]
    fn disabled_when_ratio_negative_treated_as_disabled() {
        let decision = cq(-1.0).should_step_down(Some(Duration::from_secs(60)), Duration::from_secs(60));
        assert_eq!(decision, CheckQuorumDecision::Disabled);
    }

    #[test]
    fn healthy_when_recent_ack() {
        // election_timeout_max=1000, ratio=0.75 → deadline=750ms.
        // Elapsed 500ms since ack → healthy.
        let decision = cq(0.75).should_step_down(Some(Duration::from_millis(500)), Duration::from_secs(10));
        assert_eq!(decision, CheckQuorumDecision::Healthy);
        assert!(!decision.must_step_down());
    }

    #[test]
    fn step_down_when_ack_too_old() {
        // deadline=750ms, elapsed=1000ms → step down.
        let decision = cq(0.75).should_step_down(Some(Duration::from_millis(1000)), Duration::from_secs(10));
        assert!(matches!(decision, CheckQuorumDecision::StepDown { .. }));
        assert!(decision.must_step_down());
    }

    #[test]
    fn step_down_at_exactly_ferrosa_default_ratio() {
        // The ferrosa default per ADR-012: 0.75 of 6000ms election timeout = 4500ms.
        let cq = CheckQuorum {
            ratio: 0.75,
            election_timeout_max_ms: 6000,
        };
        let decision = cq.should_step_down(Some(Duration::from_millis(4501)), Duration::from_secs(10));
        match decision {
            CheckQuorumDecision::StepDown { elapsed, deadline } => {
                assert_eq!(deadline, Duration::from_millis(4500));
                assert_eq!(elapsed, Duration::from_millis(4501));
            }
            other => panic!("expected StepDown, got {:?}", other),
        }
    }

    #[test]
    fn never_acked_grace_period_then_step_down() {
        // deadline=750ms. Just elected, no acks yet.
        let cq = cq(0.75);
        // Inside grace period (elapsed_since_election=300ms).
        let decision = cq.should_step_down(None, Duration::from_millis(300));
        assert!(matches!(decision, CheckQuorumDecision::NeverAcked { .. }));
        assert!(!decision.must_step_down());

        // Past grace period (elapsed_since_election=800ms > 750ms deadline).
        let decision = cq.should_step_down(None, Duration::from_millis(800));
        assert!(matches!(decision, CheckQuorumDecision::NeverAcked { .. }));
        assert!(decision.must_step_down(), "must step down once grace expires");
    }

    #[test]
    fn ratio_one_equals_etcd_behavior() {
        // ratio=1.0 → deadline = full election timeout. Matches etcd default.
        let cq = CheckQuorum {
            ratio: 1.0,
            election_timeout_max_ms: 1000,
        };
        let healthy = cq.should_step_down(Some(Duration::from_millis(999)), Duration::from_secs(10));
        assert_eq!(healthy, CheckQuorumDecision::Healthy);

        let stepdown = cq.should_step_down(Some(Duration::from_millis(1001)), Duration::from_secs(10));
        assert!(matches!(stepdown, CheckQuorumDecision::StepDown { .. }));
    }
}
