//! TimeoutNow message types (Ongaro §3.10).
//!
//! Per ADR-012 of ferrosa: a `TimeoutNowRequest` is sent by a leader during
//! Leadership Transfer to instruct a specific follower to immediately start an
//! election (skipping the election timer AND the PreVote phase, since the
//! transfer is by trusted leader directive).
//!
//! Receiving follower-side: on receipt, if `req.vote.term >= self.current_term`
//! and the follower is up-to-date, it transitions directly to Candidate and
//! starts a regular election.

use std::borrow::Borrow;
use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::Vote;

/// `TimeoutNow` RPC sent by a leader transferring leadership to direct a
/// specific follower to immediately start an election.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct TimeoutNowRequest<NID: NodeId> {
    /// The leader's current `vote`, sent so the follower can verify the
    /// instruction comes from a current authoritative leader.
    pub vote: Vote<NID>,

    /// The leader's last log id at transfer time. The follower may use this
    /// to verify it has caught up before starting the election.
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> fmt::Display for TimeoutNowRequest<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{timeout_now vote:{}, last_log:{}}}", self.vote, self.last_log_id.display())
    }
}

impl<NID: NodeId> MessageSummary<TimeoutNowRequest<NID>> for TimeoutNowRequest<NID> {
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID: NodeId> TimeoutNowRequest<NID> {
    pub fn new(vote: Vote<NID>, last_log_id: Option<LogId<NID>>) -> Self {
        Self { vote, last_log_id }
    }
}

/// Response to a `TimeoutNowRequest`.
///
/// Mostly an ack; the actual leadership change is observed by the leader via
/// `RaftMetrics` watching for the new term/leader.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct TimeoutNowResponse<NID: NodeId> {
    /// The follower's current `vote` after handling the directive.
    pub vote: Vote<NID>,

    /// Whether the follower accepted the directive and started an election.
    /// `false` if it declined (e.g., its log is behind the leader's, or it
    /// no longer considers the sender the legitimate leader).
    pub started_election: bool,
}

impl<NID: NodeId> MessageSummary<TimeoutNowResponse<NID>> for TimeoutNowResponse<NID> {
    fn summary(&self) -> String {
        format!("{{timeout_now {}, started:{}}}", self.vote, self.started_election)
    }
}

impl<NID> TimeoutNowResponse<NID>
where NID: NodeId
{
    pub fn new(vote: impl Borrow<Vote<NID>>, started_election: bool) -> Self {
        Self {
            vote: vote.borrow().clone(),
            started_election,
        }
    }
}
