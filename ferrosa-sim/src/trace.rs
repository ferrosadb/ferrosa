//! Trace events emitted by the simulator.
//!
//! Sprint 5 W5.4 introduces these so the W5.4 test can compare two
//! runs of the same seed for byte-equivalence.  W5.10 reuses the
//! same shape, tagged with TLA+ action names, to feed the refinement
//! verifier.
//!
//! The trace is intentionally pure: each entry is `(tick, action)`
//! where `action` is a `TlaAction` — the same names appear as
//! actions in `specs/tla/raft.tla`.

use crate::node::NodeId;
use serde::{Deserialize, Serialize};

/// One TLA+ action observed during simulation.
///
/// Names match `specs/tla/raft.tla` actions verbatim, so the
/// refinement check (W5.10) can route each step to the matching
/// transition predicate.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum TlaAction {
    /// `BecomeCandidate(n, t)` — node `n` enters term `t` as a
    /// candidate.
    BecomeCandidate {
        /// Node that became a candidate.
        node: NodeId,
        /// New term.
        term: u64,
    },
    /// `RequestVote(from, to, t)` — vote request crosses the wire.
    RequestVote {
        /// Candidate.
        from: NodeId,
        /// Voter.
        to: NodeId,
        /// Candidate's term.
        term: u64,
    },
    /// `GrantVote(to, from, t)` — voter `to` granted its vote to
    /// `from` at term `t`.
    GrantVote {
        /// Voter.
        from: NodeId,
        /// Candidate that received the vote.
        to: NodeId,
        /// Term at which the vote was granted.
        term: u64,
    },
    /// `RejectVote(to, from, t)` — voter `to` rejected `from`'s
    /// request at term `t`.
    RejectVote {
        /// Voter.
        from: NodeId,
        /// Candidate that was rejected.
        to: NodeId,
        /// Term at which the rejection happened.
        term: u64,
    },
    /// `BecomeLeader(n, t)` — node `n` won an election in term `t`.
    BecomeLeader {
        /// Newly elected leader.
        node: NodeId,
        /// Leader's term.
        term: u64,
    },
    /// `BecomeFollower(n, t)` — node `n` stepped down to a higher
    /// term.
    BecomeFollower {
        /// Node that stepped down.
        node: NodeId,
        /// New term.
        term: u64,
    },
    /// `AppendEntries(from, to, t)` — heartbeat or replication
    /// message.
    AppendEntries {
        /// Leader.
        from: NodeId,
        /// Follower.
        to: NodeId,
        /// Leader's term.
        term: u64,
    },
}

/// One scheduled trace entry.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceEntry {
    /// Simulated tick at which the action fired.
    pub tick: u64,
    /// The action itself.
    pub action: TlaAction,
}

/// Append-only ordered list of [`TraceEntry`].
///
/// Two simulator runs with the same seed must produce equal
/// `Trace`s — this is the determinism contract verified by the
/// W5.4 test.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    /// Backing storage.
    pub entries: Vec<TraceEntry>,
}

impl Trace {
    /// Empty trace.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `action` at `tick`.
    pub fn push(&mut self, tick: u64, action: TlaAction) {
        self.entries.push(TraceEntry { tick, action });
    }

    /// Number of recorded entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// `true` iff no entries have been recorded.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
