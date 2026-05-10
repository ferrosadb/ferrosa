//! `SimulatedCluster` — N [`SimulatedNode`]s sharing a deterministic
//! event loop.
//!
//! Sprint 5 W5.3.  The cluster is the unit of test composition: a
//! seed in, a final state out.  All progress comes from
//! [`SimulatedCluster::tick`], which advances the simulated clock by
//! one millisecond and drains every event whose deadline has passed.
//!
//! W5.3 implements the smallest event surface that lets a 3-voter
//! cluster elect a leader:
//!
//! - `ElectionTimeout(node)` — fired when a follower hasn't heard
//!   from a leader within its randomized timeout window.  Causes the
//!   node to become a candidate, bump term, vote for itself, and
//!   broadcast `RequestVote` to every peer.
//! - `RequestVote(from, to, term, last_log_index, last_log_term)` —
//!   delivered to `to`; granted iff the term is at least the
//!   recipient's current term and the log is at least as up-to-date.
//! - `RequestVoteReply(from, to, term, granted)` — counted by the
//!   candidate; on majority, the candidate becomes leader and starts
//!   sending heartbeats.
//! - `Heartbeat(from, to, term)` — empty `AppendEntries`.  Resets
//!   the recipient's election timer.
//!
//! The model deliberately starts at a level above the wire protocol
//! to keep the simulator small.  Sprint 5 W5.7+ extends it with
//! AppendEntries log replication, joint consensus, snapshots.

use crate::deployment::DeploymentMode;
use crate::node::{NodeId, Role, SimulatedNode};
use crate::rng::SeededRng;
use crate::trace::{TlaAction, Trace};
use std::collections::{BTreeMap, BinaryHeap};

/// One unit of simulated time.  All deadlines are stored as a count
/// of ticks; a tick is conceptually one millisecond.
pub type Tick = u64;

/// Default heartbeat interval, in ticks.
pub const HEARTBEAT_TICKS: Tick = 50;
/// Lower bound of the randomized election timeout.
pub const ELECTION_TIMEOUT_MIN: Tick = 200;
/// Upper bound of the randomized election timeout.
pub const ELECTION_TIMEOUT_MAX: Tick = 400;

/// One in-flight or scheduled simulator event.
#[derive(Clone, Debug)]
pub enum Event {
    /// The recipient's election timer has expired; it should start a
    /// candidacy.
    ElectionTimeout {
        /// Node whose election timer fired.
        node: NodeId,
    },
    /// `from` asks `to` for a vote at `term`.
    RequestVote {
        /// Candidate that issued the request.
        from: NodeId,
        /// Voter being asked.
        to: NodeId,
        /// Candidate's term.
        term: u64,
        /// Candidate's last log index (Raft log up-to-date check).
        last_log_index: u64,
        /// Candidate's last log term (Raft log up-to-date check).
        last_log_term: u64,
    },
    /// Reply to a previous [`Event::RequestVote`].
    RequestVoteReply {
        /// Voter that produced the reply.
        from: NodeId,
        /// Candidate the reply is delivered to.
        to: NodeId,
        /// Voter's term at the moment the reply was created.
        term: u64,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// Empty AppendEntries.  Resets `to`'s election timer.
    Heartbeat {
        /// Leader that emitted the heartbeat.
        from: NodeId,
        /// Follower receiving it.
        to: NodeId,
        /// Leader's term.
        term: u64,
    },
}

/// Wraps an [`Event`] with its scheduled firing tick.
#[derive(Clone, Debug)]
struct Scheduled {
    deadline: Tick,
    /// Monotonic insertion counter.  Used as the secondary key in
    /// the priority queue so two events with the same deadline fire
    /// in deterministic insertion order — never an indeterminate
    /// hash- or pointer-based tiebreak.
    seq: u64,
    event: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}

impl Eq for Scheduled {}

impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scheduled {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is max-heap; flip both keys so the *earliest*
        // deadline (and lowest seq, on tie) fires first.
        other
            .deadline
            .cmp(&self.deadline)
            .then(other.seq.cmp(&self.seq))
    }
}

/// Live state recorded for one node in the cluster.
///
/// Carries the raw [`SimulatedNode`] plus per-node bookkeeping the
/// event loop needs (election timer, votes received this term).
#[derive(Clone, Debug)]
struct NodeState {
    node: SimulatedNode,
    /// Tick at which this node will time out and start an election
    /// (or `None` if it is the leader / has been silenced by tests).
    election_deadline: Option<Tick>,
    /// Set of voters that have granted us a vote in
    /// `node.term` — populated only while `node.role == Candidate`.
    votes_received: std::collections::BTreeSet<NodeId>,
}

/// Aggregate of N [`SimulatedNode`]s sharing a deterministic clock.
pub struct SimulatedCluster {
    /// Per-voter state, keyed by [`NodeId`].
    nodes: BTreeMap<NodeId, NodeState>,
    /// Pending events ordered by deadline.
    queue: BinaryHeap<Scheduled>,
    /// Current simulated tick.
    now: Tick,
    /// Monotonic counter feeding [`Scheduled::seq`].
    next_seq: u64,
    /// Seeded RNG for randomized election timeouts.
    rng: SeededRng,
    /// Append-only trace of every TLA+ action observed.
    trace: Trace,
}

impl SimulatedCluster {
    /// Build an `n`-voter cluster seeded by `seed`.  Voter ids are
    /// `1..=n`.  Every node starts with a randomized election
    /// deadline so that a single seed picks the first leader.
    pub fn with_voters(n: u32, seed: u64) -> Self {
        assert!(n >= 1, "cluster needs at least one voter");
        let mut nodes = BTreeMap::new();
        let mut rng = SeededRng::new(seed);
        let mut queue = BinaryHeap::new();
        let mut next_seq = 0_u64;

        for id in 1..=u64::from(n) {
            let mut node = SimulatedNode::new(id);
            // A fresh sim node enters the loop already in the
            // Follower role — this skips the bootstrap-mode prefix
            // for clarity in W5.3 and matches Raft's initial state.
            node.role = Role::Follower;
            let election =
                ELECTION_TIMEOUT_MIN + rng.gen_range(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN);
            queue.push(Scheduled {
                deadline: election,
                seq: next_seq,
                event: Event::ElectionTimeout { node: id },
            });
            next_seq += 1;
            nodes.insert(
                id,
                NodeState {
                    node,
                    election_deadline: Some(election),
                    votes_received: Default::default(),
                },
            );
        }

        Self {
            nodes,
            queue,
            now: 0,
            next_seq,
            rng,
            trace: Trace::new(),
        }
    }

    /// Borrow the action trace recorded so far.
    ///
    /// W5.4 reproducibility: same seed = same `Trace`.  W5.10
    /// refinement: each [`TlaAction`] is checked against the spec.
    pub fn trace(&self) -> &Trace {
        &self.trace
    }

    /// Number of voters in the cluster.
    pub fn voter_count(&self) -> usize {
        self.nodes.len()
    }

    /// Reference the [`SimulatedNode`] for `id`, panicking if absent.
    pub fn node(&self, id: NodeId) -> &SimulatedNode {
        &self.nodes[&id].node
    }

    /// Current simulated tick.
    pub fn now(&self) -> Tick {
        self.now
    }

    /// Run the event loop until either there's a leader at term ≥ 1
    /// or `deadline` is reached.  Returns the leader's id on success.
    pub fn run_until_leader(&mut self, deadline: Tick) -> Option<NodeId> {
        while self.now <= deadline {
            if let Some(leader) = self.leader() {
                return Some(leader);
            }
            if !self.step() {
                break;
            }
        }
        self.leader()
    }

    /// First node currently in [`Role::Leader`], if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|n| n.node.role == Role::Leader)
            .map(|n| n.node.id)
    }

    /// Compute the [`DeploymentMode`] that an external observer of
    /// node `id` would report.
    ///
    /// Mirrors `ferrosa-cluster::mode::DeploymentMode::from_peer_count`
    /// — once a leader exists in a multi-voter cluster, every voter
    /// reports `Cluster`.
    ///
    /// `id` is accepted for symmetry with future per-node modes
    /// (e.g. `DegradedPair` after a partition); the W5.3 mapping is
    /// global because every voter agrees on the leader's existence.
    pub fn deployment_mode(&self, id: NodeId) -> DeploymentMode {
        let _ = id;
        let peer_count = self.voter_count().saturating_sub(1);
        if self.leader().is_some() {
            return DeploymentMode::from_peer_count(peer_count);
        }
        // No leader yet: report the bootstrap mode.
        match peer_count {
            0 => DeploymentMode::Standalone,
            1 => DeploymentMode::Pair,
            _ => DeploymentMode::Forming,
        }
    }

    // -------------------------------------------------------------
    // Internal event loop.
    // -------------------------------------------------------------

    /// Pop and dispatch the earliest pending event.  Returns `false`
    /// when the queue is empty (the cluster has reached steady state
    /// — for W5.3, that's "leader elected and heartbeats settled").
    fn step(&mut self) -> bool {
        let Some(next) = self.queue.pop() else {
            return false;
        };
        self.now = self.now.max(next.deadline);
        match next.event {
            Event::ElectionTimeout { node } => self.on_election_timeout(node),
            Event::RequestVote {
                from,
                to,
                term,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, to, term, last_log_index, last_log_term),
            Event::RequestVoteReply {
                from,
                to,
                term,
                granted,
            } => self.on_request_vote_reply(from, to, term, granted),
            Event::Heartbeat { from, to, term } => self.on_heartbeat(from, to, term),
        }
        true
    }

    fn schedule(&mut self, delay: Tick, event: Event) {
        let deadline = self.now + delay;
        self.queue.push(Scheduled {
            deadline,
            seq: self.next_seq,
            event,
        });
        self.next_seq += 1;
    }

    fn peer_ids(&self, except: NodeId) -> Vec<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|id| *id != except)
            .collect()
    }

    fn quorum_size(&self) -> usize {
        self.voter_count() / 2 + 1
    }

    fn on_election_timeout(&mut self, id: NodeId) {
        // Stale timeout?  Ignore — the election deadline may have
        // moved when a heartbeat arrived.
        let stale = self
            .nodes
            .get(&id)
            .and_then(|s| s.election_deadline)
            .map(|d| d != self.now)
            .unwrap_or(true);
        if stale {
            return;
        }

        let peers = self.peer_ids(id);
        let new_timeout = self.now + self.random_election_timeout();
        let (term, last_log_index, last_log_term);
        {
            let state = self.nodes.get_mut(&id).expect("node exists");
            // Become candidate.
            state.node.term += 1;
            state.node.role = Role::Candidate;
            state.node.voted_for = Some(id);
            state.votes_received.clear();
            state.votes_received.insert(id);
            state.election_deadline = Some(new_timeout);
            term = state.node.term;
            last_log_index = state.node.log_len;
            last_log_term = state.node.term.saturating_sub(1);
        }
        self.trace
            .push(self.now, TlaAction::BecomeCandidate { node: id, term });

        // Re-arm the election timer (in case the vote round fails).
        let next_timeout = self.nodes[&id].election_deadline.unwrap();
        self.queue.push(Scheduled {
            deadline: next_timeout,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: id },
        });
        self.next_seq += 1;

        // Broadcast RequestVote to every peer.
        for peer in peers {
            self.schedule(
                1,
                Event::RequestVote {
                    from: id,
                    to: peer,
                    term,
                    last_log_index,
                    last_log_term,
                },
            );
            self.trace.push(
                self.now,
                TlaAction::RequestVote {
                    from: id,
                    to: peer,
                    term,
                },
            );
        }

        // If a single-voter cluster, immediate self-majority.
        if self.voter_count() == 1 {
            self.become_leader(id);
        }
    }

    fn on_request_vote(
        &mut self,
        from: NodeId,
        to: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        // Roll the RNG up front — it's a `&mut self` method and we
        // need an active borrow on `self.nodes` below.
        let new_timeout = self.now + self.random_election_timeout();
        let granted;
        let stepped_down;
        let final_term;
        {
            let state = self.nodes.get_mut(&to).expect("node exists");
            // §5.1: if the request term is *higher*, step down.
            stepped_down = term > state.node.term;
            if stepped_down {
                state.node.term = term;
                state.node.role = Role::Follower;
                state.node.voted_for = None;
            }
            let log_ok = last_log_term > state.node.term.saturating_sub(1)
                || (last_log_term == state.node.term.saturating_sub(1)
                    && last_log_index >= state.node.log_len);
            let can_grant = term == state.node.term
                && (state.node.voted_for.is_none() || state.node.voted_for == Some(from))
                && log_ok;
            if can_grant {
                state.node.voted_for = Some(from);
                state.election_deadline = Some(new_timeout);
            }
            granted = can_grant;
            final_term = state.node.term;
        }
        if stepped_down {
            self.trace.push(
                self.now,
                TlaAction::BecomeFollower {
                    node: to,
                    term: final_term,
                },
            );
        }
        self.trace.push(
            self.now,
            if granted {
                TlaAction::GrantVote {
                    from: to,
                    to: from,
                    term,
                }
            } else {
                TlaAction::RejectVote {
                    from: to,
                    to: from,
                    term,
                }
            },
        );

        // Re-arm the recipient's timer if the deadline moved.
        if granted {
            let new_deadline = self.nodes[&to].election_deadline.unwrap();
            self.queue.push(Scheduled {
                deadline: new_deadline,
                seq: self.next_seq,
                event: Event::ElectionTimeout { node: to },
            });
            self.next_seq += 1;
        }

        let reply_term = self.nodes[&to].node.term;
        self.schedule(
            1,
            Event::RequestVoteReply {
                from: to,
                to: from,
                term: reply_term,
                granted,
            },
        );
    }

    fn on_request_vote_reply(&mut self, from: NodeId, to: NodeId, term: u64, granted: bool) {
        let majority;
        {
            let state = self.nodes.get_mut(&to).expect("node exists");
            // Stale reply (term moved on, or we are no longer a
            // candidate)?  Ignore.
            if state.node.role != Role::Candidate || term != state.node.term {
                return;
            }
            if granted {
                state.votes_received.insert(from);
            }
            majority = state.votes_received.len() >= self.quorum_size();
        }
        if majority {
            self.become_leader(to);
        }
    }

    fn on_heartbeat(&mut self, _from: NodeId, to: NodeId, term: u64) {
        let new_timeout = self.now + self.random_election_timeout();
        let stepped_down;
        let final_term;
        {
            let state = self.nodes.get_mut(&to).expect("node exists");
            if term < state.node.term {
                return; // stale leader
            }
            stepped_down = term > state.node.term || state.node.role != Role::Follower;
            if term > state.node.term {
                state.node.term = term;
                state.node.voted_for = None;
            }
            state.node.role = Role::Follower;
            state.election_deadline = Some(new_timeout);
            final_term = state.node.term;
        }
        let new_deadline = self.nodes[&to].election_deadline.unwrap();
        self.queue.push(Scheduled {
            deadline: new_deadline,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: to },
        });
        self.next_seq += 1;
        if stepped_down {
            self.trace.push(
                self.now,
                TlaAction::BecomeFollower {
                    node: to,
                    term: final_term,
                },
            );
        }
    }

    fn become_leader(&mut self, id: NodeId) {
        let term;
        {
            let state = self.nodes.get_mut(&id).expect("node exists");
            state.node.role = Role::Leader;
            state.election_deadline = None;
            term = state.node.term;
        }
        self.trace
            .push(self.now, TlaAction::BecomeLeader { node: id, term });
        // Immediately broadcast a heartbeat so followers latch.
        for peer in self.peer_ids(id) {
            self.schedule(
                1,
                Event::Heartbeat {
                    from: id,
                    to: peer,
                    term,
                },
            );
            self.trace.push(
                self.now,
                TlaAction::AppendEntries {
                    from: id,
                    to: peer,
                    term,
                },
            );
        }
    }

    fn random_election_timeout(&mut self) -> Tick {
        ELECTION_TIMEOUT_MIN
            + self
                .rng
                .gen_range(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// W5.3 RED → GREEN: a 3-voter cluster reaches a leader within
    /// bounded simulated time, and every voter reports
    /// `DeploymentMode::Cluster` once it does.
    #[test]
    fn madsim_runs_3_node_to_cluster() {
        let mut cluster = SimulatedCluster::with_voters(3, 42);
        let leader = cluster
            .run_until_leader(10_000)
            .expect("3-voter cluster must elect a leader within 10s simulated");
        assert!(matches!(leader, 1..=3));
        for id in [1, 2, 3] {
            assert_eq!(cluster.deployment_mode(id), DeploymentMode::Cluster);
        }
    }

    /// W5.4 RED → GREEN: two cluster runs with the same seed produce
    /// identical traces, byte-for-byte.  This is the determinism
    /// contract that the TLA+ refinement check (W5.10) and the
    /// nightly 100K-seed workflow (W5.11) both rely on.
    #[test]
    fn same_seed_produces_same_trace() {
        let mut a = SimulatedCluster::with_voters(3, 42);
        let mut b = SimulatedCluster::with_voters(3, 42);
        let _la = a.run_until_leader(10_000).unwrap();
        let _lb = b.run_until_leader(10_000).unwrap();
        assert_eq!(a.trace(), b.trace());
        assert!(!a.trace().is_empty());
    }

    /// Different seeds should *eventually* produce different traces.
    /// This is a soft guarantee — splitmix64 is statistically strong
    /// over a few hundred draws.  The test pins the contract: same
    /// seed = same trace; different seed = different trace.
    #[test]
    fn different_seeds_produce_different_traces() {
        let mut a = SimulatedCluster::with_voters(3, 1);
        let mut b = SimulatedCluster::with_voters(3, 2);
        let _ = a.run_until_leader(10_000).unwrap();
        let _ = b.run_until_leader(10_000).unwrap();
        assert_ne!(a.trace(), b.trace());
    }
}
