//! PreVote message types (Ongaro §9.6).
//!
//! Per ADR-012 of ferrosa: a `PreVoteRequest` is a probe sent by a node that
//! suspects the leader is dead and is *considering* starting an election. The
//! probe asks "would you grant me a vote if I held a real election?" without
//! mutating any persistent `Vote` state on either side.
//!
//! Distinct from `VoteRequest`/`VoteResponse` so receivers can apply
//! lease-aware rejection (reject if the receiver heard from the leader within
//! the last election timeout) without conflating the two state machines.

use std::borrow::Borrow;
use std::fmt;

use crate::display_ext::DisplayOptionExt;
use crate::LogId;
use crate::MessageSummary;
use crate::NodeId;
use crate::Vote;

/// PreVote probe sent by a PreCandidate before incrementing its term and
/// starting a real election.
///
/// Carries the candidate's *prospective* `Vote` (i.e., what it would campaign
/// with if pre-grants succeed) and `last_log_id` (used for the standard
/// election-restriction check).
///
/// Receivers must NOT mutate persistent `Vote` state when handling this.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct PreVoteRequest<NID: NodeId> {
    /// The vote the candidate would use if its pre-vote round succeeds.
    pub vote: Vote<NID>,

    /// The candidate's last log id, used for the election-restriction check
    /// (Raft §5.4.1: a voter only grants if candidate is at-least-as-up-to-date).
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> fmt::Display for PreVoteRequest<NID> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{prevote vote:{}, last_log:{}}}", self.vote, self.last_log_id.display(),)
    }
}

impl<NID: NodeId> MessageSummary<PreVoteRequest<NID>> for PreVoteRequest<NID> {
    fn summary(&self) -> String {
        self.to_string()
    }
}

impl<NID: NodeId> PreVoteRequest<NID> {
    pub fn new(vote: Vote<NID>, last_log_id: Option<LogId<NID>>) -> Self {
        Self { vote, last_log_id }
    }
}

/// Response to a `PreVoteRequest`.
///
/// `vote_granted == true` means the receiver would grant a real vote if asked
/// right now. `vote_granted == false` means either:
///   - the candidate's `last_log_id` is stale (election restriction), or
///   - the receiver has heard from the current leader within the last
///     `election_timeout` (lease-aware rejection — Ongaro §9.6 caveat).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize), serde(bound = ""))]
pub struct PreVoteResponse<NID: NodeId> {
    /// The receiver's current `vote`. Always `>=` the request's vote.
    /// Note: pre-vote handling does not mutate `Vote` state — this is the
    /// *current* vote, used by the candidate to detect "I am stale, don't
    /// bother starting a real election."
    pub vote: Vote<NID>,

    /// Whether the receiver would grant a real vote.
    pub vote_granted: bool,

    /// The receiver's last log id, used for additional candidate diagnostics.
    pub last_log_id: Option<LogId<NID>>,
}

impl<NID: NodeId> MessageSummary<PreVoteResponse<NID>> for PreVoteResponse<NID> {
    fn summary(&self) -> String {
        format!(
            "{{prevote {}, granted:{}, last_log:{:?}}}",
            self.vote,
            self.vote_granted,
            self.last_log_id.as_ref().map(|x| x.to_string())
        )
    }
}

impl<NID> PreVoteResponse<NID>
where NID: NodeId
{
    pub fn new(vote: impl Borrow<Vote<NID>>, last_log_id: Option<LogId<NID>>, granted: bool) -> Self {
        Self {
            vote: vote.borrow().clone(),
            vote_granted: granted,
            last_log_id,
        }
    }
}
