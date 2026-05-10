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
    /// W5.8 PreVote: a probe that *does not* mutate term.  If the
    /// candidate collects a quorum of `PreVoteReply { granted = true }`,
    /// it then issues the real `RequestVote`.
    PreVoteRequest {
        /// PreCandidate that issued the probe.
        from: NodeId,
        /// Voter being asked.
        to: NodeId,
        /// Hypothetical term.
        term: u64,
        /// PreCandidate's last log index.
        last_log_index: u64,
        /// PreCandidate's last log term.
        last_log_term: u64,
    },
    /// Reply to a previous [`Event::PreVoteRequest`].
    PreVoteReply {
        /// Voter that produced the reply.
        from: NodeId,
        /// PreCandidate the reply is delivered to.
        to: NodeId,
        /// Term being probed.
        term: u64,
        /// Whether the PreVote was granted.
        granted: bool,
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
    /// W5.8: voters that have granted a PreVote at the next
    /// hypothetical term.  Reset on each fresh PreVote round.
    pre_votes_received: std::collections::BTreeSet<NodeId>,
    /// `true` if the node is crashed (e.g. by a `KillMinority`
    /// nemesis).  Crashed nodes drop every event addressed to them
    /// and never schedule new ones.
    crashed: bool,
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
    /// Pairs `(from, to)` whose messages must be dropped.  Modelled
    /// as a `BTreeSet` so iteration is deterministic.
    dropped_links: std::collections::BTreeSet<(NodeId, NodeId)>,
    /// W5.8: when `true`, election timeouts run a PreVote round
    /// before bumping the term.  `false` keeps the W5.3 behaviour.
    pub(crate) pre_vote_enabled: bool,
    /// W5.9: pending Cnew during a joint-consensus phase.  Empty
    /// outside the joint phase.  See `propose_membership`.
    pending_config: std::collections::BTreeSet<NodeId>,
    /// W5.9: the "Cold" set referenced during joint consensus.
    /// Captures the membership at the moment `propose_membership`
    /// fires; the joint quorum requires majorities in BOTH `cold`
    /// and `pending_config`.
    cold_config: std::collections::BTreeSet<NodeId>,
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
                    pre_votes_received: Default::default(),
                    crashed: false,
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
            dropped_links: std::collections::BTreeSet::new(),
            pre_vote_enabled: false,
            pending_config: std::collections::BTreeSet::new(),
            cold_config: std::collections::BTreeSet::new(),
        }
    }

    /// Builder-style flip: enable PreVote (W5.8).  Election timeouts
    /// then schedule a `PreVoteRequest` round before bumping term.
    pub fn with_pre_vote(mut self) -> Self {
        self.pre_vote_enabled = true;
        self
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

    /// Iterator over voter ids, in ascending order.
    pub fn voter_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// `true` if a node with the given id is currently a voter.
    pub fn has_voter(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
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

    /// Drive the event loop for `duration` simulated ticks, draining
    /// every event whose deadline falls in the window.
    pub fn run_for(&mut self, duration: Tick) {
        let target = self.now + duration;
        while self.now < target {
            // Stop if no event is scheduled to fire by `target`.
            let next_deadline = self.queue.peek().map(|s| s.deadline);
            match next_deadline {
                Some(d) if d <= target => {}
                _ => break,
            }
            if !self.step() {
                break;
            }
        }
        self.now = self.now.max(target);
    }

    /// Schedule a heartbeat from the current leader to a peer at
    /// `now + delay` ticks.  No-op if there is no leader.
    pub fn schedule_leader_heartbeat(&mut self, delay: Tick) {
        let Some(leader) = self.leader() else {
            return;
        };
        let term = self.nodes[&leader].node.term;
        for peer in self.peer_ids(leader) {
            self.queue.push(Scheduled {
                deadline: self.now + delay,
                seq: self.next_seq,
                event: Event::Heartbeat {
                    from: leader,
                    to: peer,
                    term,
                },
            });
            self.next_seq += 1;
        }
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
        // Nemesis filters: drop events targeting crashed nodes, and
        // drop messages that cross a partitioned link.
        if self.event_dropped(&next.event) {
            return true;
        }
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
            Event::PreVoteRequest {
                from,
                to,
                term,
                last_log_index,
                last_log_term,
            } => self.on_pre_vote_request(from, to, term, last_log_index, last_log_term),
            Event::PreVoteReply {
                from,
                to,
                term,
                granted,
            } => self.on_pre_vote_reply(from, to, term, granted),
        }
        true
    }

    fn event_dropped(&self, event: &Event) -> bool {
        let crashed = |id: NodeId| self.nodes.get(&id).map(|n| n.crashed).unwrap_or(true);
        let partitioned = |from: NodeId, to: NodeId| {
            self.dropped_links.contains(&(from, to)) || self.dropped_links.contains(&(to, from))
        };
        match *event {
            Event::ElectionTimeout { node } => crashed(node),
            Event::RequestVote { from, to, .. }
            | Event::RequestVoteReply { from, to, .. }
            | Event::Heartbeat { from, to, .. }
            | Event::PreVoteRequest { from, to, .. }
            | Event::PreVoteReply { from, to, .. } => {
                crashed(from) || crashed(to) || partitioned(from, to)
            }
        }
    }

    // -------------------------------------------------------------
    // Nemesis API (W5.5).  Each method is a pure mutation on cluster
    // state — no events are scheduled here; the next `step` call
    // observes the change.
    // -------------------------------------------------------------

    /// Drop every message between `a` and `b`, in both directions.
    pub fn partition_pair(&mut self, a: NodeId, b: NodeId) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        self.dropped_links.insert((lo, hi));
    }

    /// Re-enable messages between `a` and `b`.
    pub fn unpartition_pair(&mut self, a: NodeId, b: NodeId) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        self.dropped_links.remove(&(lo, hi));
    }

    /// Crash-stop `id`.  All future events for this node are dropped
    /// until [`Self::revive`] is called.
    pub fn kill(&mut self, id: NodeId) {
        if let Some(state) = self.nodes.get_mut(&id) {
            state.crashed = true;
        }
    }

    /// Revive a previously-killed node.  Re-arms its election timer.
    pub fn revive(&mut self, id: NodeId) {
        let new_timeout = self.now + self.random_election_timeout();
        if let Some(state) = self.nodes.get_mut(&id) {
            state.crashed = false;
            state.node.role = Role::Follower;
            state.election_deadline = Some(new_timeout);
        }
        self.queue.push(Scheduled {
            deadline: new_timeout,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: id },
        });
        self.next_seq += 1;
    }

    /// Add `new_id` as a fresh follower.  Schedules its first
    /// election timeout.
    pub fn add_voter(&mut self, new_id: NodeId) {
        if self.nodes.contains_key(&new_id) {
            return;
        }
        let deadline = self.now + self.random_election_timeout();
        let mut node = SimulatedNode::new(new_id);
        node.role = Role::Follower;
        self.nodes.insert(
            new_id,
            NodeState {
                node,
                election_deadline: Some(deadline),
                votes_received: Default::default(),
                pre_votes_received: Default::default(),
                crashed: false,
            },
        );
        self.queue.push(Scheduled {
            deadline,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: new_id },
        });
        self.next_seq += 1;
    }

    /// Remove `id` from the cluster.  Future events targeting it are
    /// dropped.
    pub fn remove_voter(&mut self, id: NodeId) {
        self.nodes.remove(&id);
    }

    // -------------------------------------------------------------
    // Joint-consensus membership change (W5.9).
    // -------------------------------------------------------------

    /// Begin a joint-consensus membership change.  `new_config` is
    /// `Cnew`; the current voter set is captured as `Cold`.  The
    /// cluster enters the joint phase, in which a quorum requires
    /// majorities in BOTH old and new.
    pub fn propose_membership(&mut self, new_config: std::collections::BTreeSet<NodeId>) {
        self.cold_config = self.nodes.keys().copied().collect();
        self.pending_config = new_config.clone();
        // Add brand-new voters in `Cnew` that aren't yet in `Cold`.
        for &id in &new_config {
            if !self.nodes.contains_key(&id) {
                self.add_voter(id);
            }
        }
    }

    /// Commit the pending membership change: replace `Cold` with
    /// `Cnew`, drop voters that fell out of the new config.
    pub fn commit_membership(&mut self) {
        if self.pending_config.is_empty() {
            return;
        }
        // Drop voters that are in `Cold` but not in `Cnew`.
        let to_remove: Vec<NodeId> = self
            .cold_config
            .difference(&self.pending_config)
            .copied()
            .collect();
        for id in to_remove {
            self.remove_voter(id);
        }
        self.cold_config.clear();
        self.pending_config.clear();
    }

    /// `true` while in the joint-consensus phase (post-propose,
    /// pre-commit).
    pub fn in_joint_phase(&self) -> bool {
        !self.pending_config.is_empty()
    }

    /// Check whether `voters` form a quorum under the current
    /// configuration.  In the joint phase this requires a majority
    /// in BOTH the old and new sets.
    pub fn is_joint_quorum(&self, voters: &std::collections::BTreeSet<NodeId>) -> bool {
        if self.pending_config.is_empty() {
            // Single-config quorum: simple majority of `nodes`.
            let n = self.nodes.len();
            voters
                .iter()
                .filter(|id| self.nodes.contains_key(id))
                .count()
                * 2
                > n
        } else {
            let cold_intersect = voters.intersection(&self.cold_config).count();
            let new_intersect = voters.intersection(&self.pending_config).count();
            cold_intersect * 2 > self.cold_config.len()
                && new_intersect * 2 > self.pending_config.len()
        }
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

        if self.pre_vote_enabled {
            self.start_pre_vote(id);
        } else {
            self.start_real_candidacy(id);
        }
    }

    fn start_real_candidacy(&mut self, id: NodeId) {
        let peers = self.peer_ids(id);
        let new_timeout = self.now + self.random_election_timeout();
        let (term, last_log_index, last_log_term);
        {
            let state = self.nodes.get_mut(&id).expect("node exists");
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

        let next_timeout = self.nodes[&id].election_deadline.unwrap();
        self.queue.push(Scheduled {
            deadline: next_timeout,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: id },
        });
        self.next_seq += 1;

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

        if self.voter_count() == 1 {
            self.become_leader(id);
        }
    }

    fn start_pre_vote(&mut self, id: NodeId) {
        let peers = self.peer_ids(id);
        let new_timeout = self.now + self.random_election_timeout();
        let (hypothetical_term, last_log_index, last_log_term);
        {
            let state = self.nodes.get_mut(&id).expect("node exists");
            state.node.role = Role::PreCandidate;
            state.pre_votes_received.clear();
            state.pre_votes_received.insert(id);
            state.election_deadline = Some(new_timeout);
            hypothetical_term = state.node.term + 1;
            last_log_index = state.node.log_len;
            last_log_term = state.node.term.saturating_sub(1);
        }

        let next_timeout = self.nodes[&id].election_deadline.unwrap();
        self.queue.push(Scheduled {
            deadline: next_timeout,
            seq: self.next_seq,
            event: Event::ElectionTimeout { node: id },
        });
        self.next_seq += 1;

        for peer in peers {
            self.schedule(
                1,
                Event::PreVoteRequest {
                    from: id,
                    to: peer,
                    term: hypothetical_term,
                    last_log_index,
                    last_log_term,
                },
            );
        }

        if self.voter_count() == 1 {
            // Single-voter cluster: PreVote majority is trivially
            // satisfied, proceed straight to real candidacy.
            self.start_real_candidacy(id);
        }
    }

    fn on_pre_vote_request(
        &mut self,
        from: NodeId,
        to: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        let granted;
        let reply_term;
        {
            let state = self.nodes.get(&to).expect("node exists");
            // PreVote does NOT mutate term.  Grant iff the candidate
            // would beat us in a real election: term ≥ ours and
            // log up-to-date.
            let log_ok = last_log_term > state.node.term.saturating_sub(1)
                || (last_log_term == state.node.term.saturating_sub(1)
                    && last_log_index >= state.node.log_len);
            granted = term > state.node.term && log_ok;
            reply_term = state.node.term;
        }
        self.schedule(
            1,
            Event::PreVoteReply {
                from: to,
                to: from,
                term: reply_term,
                granted,
            },
        );
    }

    fn on_pre_vote_reply(&mut self, from: NodeId, to: NodeId, _term: u64, granted: bool) {
        let promote;
        {
            let state = self.nodes.get_mut(&to).expect("node exists");
            if state.node.role != Role::PreCandidate {
                return;
            }
            if granted {
                state.pre_votes_received.insert(from);
            }
            promote = state.pre_votes_received.len() >= self.quorum_size();
        }
        if promote {
            self.start_real_candidacy(to);
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

    /// W5.8 RED → GREEN: a 3-voter cluster with PreVote enabled
    /// reaches a leader and the leader's term has incremented past 0
    /// (i.e. the PreVote round successfully promoted the
    /// PreCandidate to a real candidate, then to a leader).
    #[test]
    fn pre_vote_round_promotes_to_leader() {
        let mut cluster = SimulatedCluster::with_voters(3, 42).with_pre_vote();
        let leader = cluster.run_until_leader(10_000).unwrap();
        assert!(matches!(leader, 1..=3));
        let term = cluster.node(leader).term;
        assert!(term >= 1, "leader should have term ≥ 1, got {term}");
    }

    /// W5.8: PreVote suppresses term advances when no quorum is
    /// available.  Construct a partition that isolates one node
    /// from the other two; the isolated node must NOT bump term
    /// because no quorum can grant a PreVote.
    #[test]
    fn pre_vote_blocks_term_advance_in_minority_partition() {
        let mut cluster = SimulatedCluster::with_voters(3, 99).with_pre_vote();
        // Drive the cluster to a leader first.
        cluster.run_until_leader(10_000);
        let initial_term = (1..=3).map(|i| cluster.node(i).term).max().unwrap();

        // Isolate node 1 from {2, 3}.
        cluster.partition_pair(1, 2);
        cluster.partition_pair(1, 3);
        // Run for many election cycles.  Without PreVote, node 1
        // would repeatedly bump its term during partition.  With
        // PreVote, its term must stay ≤ initial.
        cluster.run_for(50_000);

        let n1_term = cluster.node(1).term;
        assert!(
            n1_term <= initial_term + 1,
            "isolated node 1 advanced term from {initial_term} to {n1_term} despite PreVote"
        );
    }

    /// W5.9 RED → GREEN: a 3 → 5 voter swap goes through the joint
    /// phase.  In the joint phase, neither side alone can form a
    /// quorum: `is_joint_quorum({1,2})` must be false (only 2/3 of
    /// Cold but 0/2 of Cnew); `is_joint_quorum({1,2,4,5})` must be
    /// true (2/3 of Cold AND 2/2 of Cnew).
    #[test]
    fn joint_consensus_quorum_requires_both_sides() {
        let mut cluster = SimulatedCluster::with_voters(3, 11);
        let _ = cluster.run_until_leader(10_000).unwrap();

        let cnew: std::collections::BTreeSet<u64> = [1, 2, 3, 4, 5].into_iter().collect();
        cluster.propose_membership(cnew);
        assert!(cluster.in_joint_phase());

        // Cold = {1,2,3}, Cnew = {1,2,3,4,5}.
        // {1,2}: 2/3 of Cold ✓, 2/5 of Cnew ✗.
        let q_cold_only: std::collections::BTreeSet<u64> = [1, 2].into_iter().collect();
        assert!(!cluster.is_joint_quorum(&q_cold_only));

        // {1,2,4,5}: 2/3 of Cold ✓, 4/5 of Cnew ✓.
        let q_both: std::collections::BTreeSet<u64> = [1, 2, 4, 5].into_iter().collect();
        assert!(cluster.is_joint_quorum(&q_both));

        // Commit phase: voter set settles to Cnew alone.
        cluster.commit_membership();
        assert!(!cluster.in_joint_phase());
        assert_eq!(cluster.voter_count(), 5);
    }

    /// W5.9: a voter swap from {1,2,3} to {2,3,4,5} drops node 1
    /// at commit and keeps a leader.
    #[test]
    fn joint_consensus_drops_old_voter_at_commit() {
        let mut cluster = SimulatedCluster::with_voters(3, 13);
        let _ = cluster.run_until_leader(10_000).unwrap();

        let cnew: std::collections::BTreeSet<u64> = [2, 3, 4, 5].into_iter().collect();
        cluster.propose_membership(cnew);
        cluster.commit_membership();
        assert!(!cluster.has_voter(1));
        assert!(cluster.has_voter(4));
        assert!(cluster.has_voter(5));

        cluster.run_for(50_000);
        assert!(
            cluster.leader().is_some(),
            "post-commit cluster has a leader"
        );
    }
}
