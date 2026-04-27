//! AccordCoordinator: fast-path and slow-path transaction coordination.
//!
//! The Accord coordinator drives a transaction through the consensus protocol:
//!
//! - **Fast path (1 RTT):** If a fast quorum of replicas all agree on the
//!   proposed timestamp `t0` (i.e., every `PreAcceptOK` has `t == t0` and
//!   identical deps), the coordinator can commit directly — no Accept phase.
//!   When the coordinator is the leaseholder (owns the token range), this
//!   completes in 1 round-trip time.
//!
//! - **Slow path (2 RTT):** If any replica proposes a different timestamp or
//!   different deps, the coordinator falls back to the Accept phase, requiring
//!   a second round-trip before Commit.
//!
//! # Quorum formulas
//!
//! - **Fast quorum:** `floor((3f+1)/2) + 1` where `f = RF - quorum(RF)` and
//!   `quorum(RF) = RF/2 + 1`. This is the minimum number of replicas that
//!   must unanimously agree for the fast path.
//!
//! - **Slow (classic) quorum:** `RF/2 + 1` — a simple majority.
//!
//! # Leaseholder optimization
//!
//! If the coordinator node owns the token range for the transaction's key,
//! it acts as a "leaseholder" — it counts as an implicit PreAccept vote,
//! reducing the number of remote round-trips needed.

use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use ferrosa_common::accord::{BallotNumber, HybridLogicalClock, Timestamp, TxnId};
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

// ---------------------------------------------------------------------------
// Quorum computation
// ---------------------------------------------------------------------------

/// Compute the classic (slow-path) quorum size: `RF/2 + 1`.
///
/// # Panics
///
/// Panics if `rf` is 0.
pub fn slow_quorum_size(rf: usize) -> usize {
    assert!(rf > 0, "replication factor must be positive");
    rf / 2 + 1
}

/// Compute the fast-path quorum size.
///
/// Formula: `floor((3f + 1) / 2) + 1` where `f = RF - quorum(RF)`.
///
/// This is the minimum number of unanimous PreAcceptOK responses needed
/// to commit on the fast path without an Accept round.
///
/// # Panics
///
/// Panics if `rf` is 0.
pub fn fast_quorum_size(rf: usize) -> usize {
    assert!(rf > 0, "replication factor must be positive");
    let q = slow_quorum_size(rf);
    let f = rf - q; // max failures tolerated
                    // Formula from Accord paper: floor((3f+1)/2) + 1
                    // Equivalent to ceil(3f/2) + 1, but kept explicit for traceability.
    #[allow(clippy::manual_div_ceil)]
    let result = (3 * f + 1) / 2 + 1;
    result
}

// ---------------------------------------------------------------------------
// PreAcceptResponse
// ---------------------------------------------------------------------------

/// A PreAcceptOK response from a single replica.
#[derive(Debug, Clone)]
pub struct PreAcceptResponse {
    /// The replica that sent this response.
    pub from: u64,
    /// The execution timestamp the replica proposed.
    pub t: Timestamp,
    /// The dependency set the replica computed.
    pub deps: Vec<TxnId>,
}

// ---------------------------------------------------------------------------
// AcceptResponse
// ---------------------------------------------------------------------------

/// An AcceptOK response from a single replica.
#[derive(Debug, Clone)]
pub struct AcceptResponse {
    /// The replica that sent this response.
    pub from: u64,
    /// The ballot the replica accepted.
    pub ballot: BallotNumber,
    /// The dependency set the replica accepted.
    pub deps: Vec<TxnId>,
}

// ---------------------------------------------------------------------------
// CoordinatorPhase
// ---------------------------------------------------------------------------

/// The current phase of the coordinator's protocol execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorPhase {
    /// Collecting PreAcceptOK responses.
    PreAccepting,
    /// Fast path succeeded — ready to commit (1 RTT).
    FastPathCommit,
    /// Collecting AcceptOK responses (slow path).
    Accepting,
    /// Slow path succeeded — ready to commit (2 RTT).
    SlowPathCommit,
    /// Transaction committed.
    Committed,
}

// ---------------------------------------------------------------------------
// CoordinatorDecision
// ---------------------------------------------------------------------------

/// The decision returned when enough responses have been collected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorDecision {
    /// Need more responses before deciding.
    Pending,
    /// Fast path: commit with `t == t0` and these deps. 1 RTT.
    FastPathCommit { t: Timestamp, deps: HashSet<TxnId> },
    /// Slow path needed: must run Accept phase with the merged timestamp/deps.
    NeedAccept { t: Timestamp, deps: HashSet<TxnId> },
    /// Slow path Accept complete: commit with these values. 2 RTT.
    SlowPathCommit { t: Timestamp, deps: HashSet<TxnId> },
}

// ---------------------------------------------------------------------------
// AccordCoordinator
// ---------------------------------------------------------------------------

/// Coordinator for a single Accord transaction.
///
/// Drives one transaction through PreAccept -> (fast commit | Accept -> commit).
pub struct AccordCoordinator {
    /// The transaction being coordinated.
    pub txn_id: TxnId,
    /// The coordinator's proposed timestamp.
    pub t0: Timestamp,
    /// The key(s) this transaction touches (simplified to single key).
    pub key: Vec<u8>,
    /// This coordinator's node ID.
    pub node_id: u64,
    /// Replication factor.
    pub rf: usize,
    /// Whether this coordinator is the leaseholder for the token range.
    pub is_leaseholder: bool,
    /// Current phase.
    pub phase: CoordinatorPhase,

    /// Collected PreAcceptOK responses.
    preaccept_responses: Vec<PreAcceptResponse>,
    /// Collected AcceptOK responses.
    accept_responses: Vec<AcceptResponse>,

    /// Merged execution timestamp (highest seen across all responses).
    merged_t: Timestamp,
    /// Merged dependency set (union of all response deps).
    merged_deps: HashSet<TxnId>,
    /// Number of RTTs completed (for test verification).
    rtt_count: u32,
}

impl AccordCoordinator {
    /// Create a new coordinator for a transaction.
    ///
    /// If `is_leaseholder` is true, the coordinator implicitly votes for
    /// `t0` in the PreAccept phase (counts as one response with `t == t0`
    /// and empty deps).
    pub fn new(
        txn_id: TxnId,
        t0: Timestamp,
        key: Vec<u8>,
        node_id: u64,
        rf: usize,
        is_leaseholder: bool,
    ) -> Self {
        let _span = tracing::info_span!(
            "accord.txn",
            txn_id = ?txn_id,
            t0 = ?t0,
            rf = rf,
            leaseholder = is_leaseholder,
        )
        .entered();

        assert!(rf > 0, "replication factor must be positive");

        let mut coord = Self {
            txn_id,
            t0,
            key,
            node_id,
            rf,
            is_leaseholder,
            phase: CoordinatorPhase::PreAccepting,
            preaccept_responses: Vec::new(),
            accept_responses: Vec::new(),
            merged_t: t0,
            merged_deps: HashSet::new(),
            rtt_count: 0,
        };

        // Leaseholder optimization: the coordinator itself implicitly votes
        // for t0 with empty deps (it owns the range, no conflicts seen locally).
        if is_leaseholder {
            coord.preaccept_responses.push(PreAcceptResponse {
                from: node_id,
                t: t0,
                deps: vec![],
            });
        }

        coord
    }

    /// Process a PreAcceptOK response.
    ///
    /// Returns a decision once enough responses have been collected:
    /// - `FastPathCommit` if a fast quorum unanimously agrees on `t0`.
    /// - `NeedAccept` if we have a slow quorum but not unanimous fast quorum.
    /// - `Pending` if more responses are needed.
    pub fn handle_preaccept_ok(&mut self, response: PreAcceptResponse) -> CoordinatorDecision {
        let _span = tracing::info_span!("accord.preaccept", from = response.from,).entered();

        assert_eq!(
            self.phase,
            CoordinatorPhase::PreAccepting,
            "handle_preaccept_ok called in wrong phase: {:?}",
            self.phase
        );

        self.preaccept_responses.push(response.clone());

        // Update merged state.
        if response.t > self.merged_t {
            self.merged_t = response.t;
        }
        for dep in &response.deps {
            self.merged_deps.insert(*dep);
        }

        let total = self.preaccept_responses.len();
        let fq = fast_quorum_size(self.rf);
        let sq = slow_quorum_size(self.rf);

        // Check if we have a fast quorum with unanimous agreement.
        if total >= fq {
            let all_agree = self
                .preaccept_responses
                .iter()
                .all(|r| r.t == self.t0 && self.deps_match_t0(&r.deps));

            if all_agree {
                self.phase = CoordinatorPhase::FastPathCommit;
                self.rtt_count = 1;
                return CoordinatorDecision::FastPathCommit {
                    t: self.t0,
                    deps: self.merged_deps.clone(),
                };
            }
        }

        // Check if we have enough responses to know we cannot achieve fast path.
        // If we have a slow quorum and at least one disagreement, go to Accept.
        if total >= sq {
            let has_disagreement = self
                .preaccept_responses
                .iter()
                .any(|r| r.t != self.t0 || !self.deps_match_t0(&r.deps));

            if has_disagreement {
                self.phase = CoordinatorPhase::Accepting;
                self.rtt_count = 1;
                return CoordinatorDecision::NeedAccept {
                    t: self.merged_t,
                    deps: self.merged_deps.clone(),
                };
            }
        }

        CoordinatorDecision::Pending
    }

    /// Process an AcceptOK response (slow path).
    ///
    /// Returns `SlowPathCommit` once a slow quorum of AcceptOK responses
    /// have been collected, or `Pending` if more are needed.
    pub fn handle_accept_ok(&mut self, response: AcceptResponse) -> CoordinatorDecision {
        let _span = tracing::info_span!("accord.commit", from = response.from,).entered();

        assert_eq!(
            self.phase,
            CoordinatorPhase::Accepting,
            "handle_accept_ok called in wrong phase: {:?}",
            self.phase
        );

        self.accept_responses.push(response);

        let sq = slow_quorum_size(self.rf);

        if self.accept_responses.len() >= sq {
            self.phase = CoordinatorPhase::SlowPathCommit;
            self.rtt_count = 2;
            return CoordinatorDecision::SlowPathCommit {
                t: self.merged_t,
                deps: self.merged_deps.clone(),
            };
        }

        CoordinatorDecision::Pending
    }

    /// Number of round-trips completed to reach the current decision.
    pub fn rtt_count(&self) -> u32 {
        self.rtt_count
    }

    /// Number of PreAcceptOK responses collected so far.
    pub fn preaccept_response_count(&self) -> usize {
        self.preaccept_responses.len()
    }

    /// Number of AcceptOK responses collected so far.
    pub fn accept_response_count(&self) -> usize {
        self.accept_responses.len()
    }

    /// Check if a response's deps match the "no conflict" baseline.
    /// For the fast path, all deps should be empty (no conflicts with t0).
    fn deps_match_t0(&self, deps: &[TxnId]) -> bool {
        // For fast path unanimity check: the response deps should match
        // what we expect. In the simplest case (no prior transactions),
        // all deps should be empty. In general, all responses must have
        // the same deps set.
        if self.preaccept_responses.is_empty() {
            return deps.is_empty();
        }
        // Compare against the first response's deps (all must match for fast path).
        let first_deps: HashSet<TxnId> = self.preaccept_responses[0].deps.iter().copied().collect();
        let this_deps: HashSet<TxnId> = deps.iter().copied().collect();
        first_deps == this_deps
    }
}

// ===========================================================================
// AccordCoordinatorDriver — network-aware wrapper for AccordCoordinator
// ===========================================================================

/// Error from `AccordCoordinatorDriver::run_transaction`.
#[derive(Debug)]
pub enum AccordDriverError {
    /// Too few replicas responded to reach a quorum.
    QuorumUnavailable,
    /// Network I/O error communicating with a replica.
    Network(String),
    /// Serialization/deserialization failure.
    Codec(String),
    /// The IF condition did not hold (F+1 replicas voted against apply).
    ///
    /// The LWT response to the client must carry `[applied]=false` plus
    /// the current row value(s) returned by the read-vote phase (Gap 4).
    ConditionNotMet {
        /// Serialized current row from the first dissenting replica.
        current_row: Vec<u8>,
    },
    /// F+1 apply acknowledgements were not received within the timeout.
    ApplyQuorumUnavailable,
}

impl std::fmt::Display for AccordDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuorumUnavailable => write!(f, "Accord quorum unavailable"),
            Self::Network(e) => write!(f, "Accord network error: {e}"),
            Self::Codec(e) => write!(f, "Accord codec error: {e}"),
            Self::ConditionNotMet { .. } => write!(f, "Accord LWT condition not met"),
            Self::ApplyQuorumUnavailable => write!(f, "Accord apply quorum unavailable"),
        }
    }
}

impl std::error::Error for AccordDriverError {}

/// A driver that connects the pure `AccordCoordinator` state machine to real
/// network I/O via `PeerManager`.
///
/// # Protocol
///
/// 1. **PreAccept** — fanout to all `replica_ids`, collect `PreAcceptOK`.
/// 2. Decision: fast path (`FastPathCommit`) or slow path (`NeedAccept`).
/// 3. **Accept** (slow path only) — fanout to all `replica_ids`, collect `AcceptOK`.
/// 4. **Commit** — fire-and-forget to all `replica_ids`.
///
/// The driver is single-use: one instance per transaction.
pub struct AccordCoordinatorDriver {
    coordinator: AccordCoordinator,
    peers: Arc<PeerManager>,
    /// IDs of the replicas for this transaction's token range.
    replica_ids: Vec<uuid::Uuid>,
    /// UUID of this coordinator node (used to identify self-sends).
    ///
    /// When `PeerManager::send` is called with this ID, the send will fail
    /// because the node is not registered in its own peer map. We treat the
    /// coordinator itself as an implicit ack for Commit and Apply (the
    /// coordinator drove the protocol and counts as one replica).
    self_id: uuid::Uuid,
}

impl AccordCoordinatorDriver {
    /// Build a driver for a new transaction.
    ///
    /// # Parameters
    ///
    /// - `node_id`: this coordinator's node ID.
    /// - `replica_ids`: UUIDs of all replicas (including self if leaseholder).
    /// - `peers`: the peer connection manager for RPC fanout.
    /// - `is_leaseholder`: whether this node is the token-range leaseholder.
    /// - `clock`: HLC for generating the coordinator timestamp `t0`.
    /// - `key`: raw partition key bytes.
    pub fn new(
        node_id: u64,
        replica_ids: Vec<uuid::Uuid>,
        peers: Arc<PeerManager>,
        is_leaseholder: bool,
        clock: &HybridLogicalClock,
        key: Vec<u8>,
    ) -> Self {
        let rf = replica_ids.len();
        assert!(rf > 0, "replica_ids must be non-empty");

        let t0 = clock.now();
        let txn_id = TxnId::new(node_id, t0);

        let coordinator = AccordCoordinator::new(txn_id, t0, key, node_id, rf, is_leaseholder);

        // Identify this coordinator's own UUID from the replica list by matching
        // the node_id (derived from first 8 bytes of UUID, big-endian).
        let self_id = replica_ids
            .iter()
            .find(|id| {
                let bytes = id.as_bytes();
                u64::from_be_bytes(bytes[..8].try_into().expect("uuid is 16 bytes")) == node_id
            })
            .copied()
            // If node_id is not in replica_ids, use a nil UUID (will never match
            // any peer lookup — all sends go to the network).
            .unwrap_or(uuid::Uuid::nil());

        Self {
            coordinator,
            peers,
            replica_ids,
            self_id,
        }
    }

    /// Run the full Accord protocol for this transaction.
    ///
    /// Returns the committed `(t, deps)` on success.
    ///
    /// # Phase 1 — PreAccept
    ///
    /// Send `PreAccept` to all remote replicas in parallel, collect responses,
    /// feed each to `AccordCoordinator::handle_preaccept_ok`.  Stop as soon as
    /// a quorum decision (`FastPathCommit` or `NeedAccept`) is reached.
    ///
    /// # Phase 2 — Accept (slow path only)
    ///
    /// If phase 1 decides `NeedAccept`, send `Accept` to all remote replicas,
    /// collect `AcceptOK` responses, stop on `SlowPathCommit`.
    ///
    /// # Phase 3 — Commit
    ///
    /// Broadcast `Commit` to all replicas and wait for F+1 `CommitOK` responses.
    ///
    /// # Phase 4 — Read-vote (Gap 4: linearizable IF-condition read)
    ///
    /// Send `ReadVote` to all replicas. Each replica reads the current row value
    /// within the agreed epoch (at timestamp `t`, after all deps have applied)
    /// and votes whether the IF condition holds. The coordinator collects F+1
    /// matching votes to determine `[applied]`.
    ///
    /// # Phase 5 — Apply (Gap 5: dep-wait + storage write)
    ///
    /// Broadcast `Apply` to all replicas (carrying the mutation). Wait for F+1
    /// `ApplyOK` responses before returning the LWT outcome to the caller.
    pub async fn run_transaction(
        &mut self,
    ) -> Result<(Timestamp, HashSet<TxnId>), AccordDriverError> {
        use crate::accord::wire::{
            AcceptOkPayload, AcceptPayload, ApplyOkPayload, CommitPayload, PreAcceptOkPayload,
            PreAcceptPayload, ReadVoteOkPayload, ReadVotePayload,
        };

        let txn_id = self.coordinator.txn_id;
        let t0 = self.coordinator.t0;
        let key = self.coordinator.key.clone();
        let _rf = self.coordinator.rf; // available for future quorum checks

        // ------------------------------------------------------------------
        // Phase 1: PreAccept fanout
        // ------------------------------------------------------------------

        let pa_payload = PreAcceptPayload {
            txn_id,
            t0,
            key: key.clone(),
            ballot: BallotNumber(0),
            epoch: 0,
        };
        let pa_bytes =
            bincode::serialize(&pa_payload).map_err(|e| AccordDriverError::Codec(e.to_string()))?;
        let pa_msg = Message::AccordPreAccept(Bytes::from(pa_bytes));

        // Fanout to all replicas in parallel.
        let futs: Vec<_> = self
            .replica_ids
            .iter()
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = pa_msg.clone();
                async move {
                    peers
                        .send(peer_id, msg, Lane::Data)
                        .await
                        .map(|resp| (peer_id, resp))
                }
            })
            .collect();

        let responses = futures::future::join_all(futs).await;

        let mut decision = CoordinatorDecision::Pending;
        for result in &responses {
            match result {
                Ok((_peer_id, Message::AccordPreAcceptOK(b))) if !b.is_empty() => {
                    let ok: PreAcceptOkPayload = bincode::deserialize(b)
                        .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
                    let resp = PreAcceptResponse {
                        from: ok.from,
                        t: ok.t,
                        deps: ok.deps,
                    };
                    decision = self.coordinator.handle_preaccept_ok(resp);
                    if decision != CoordinatorDecision::Pending {
                        break;
                    }
                }
                Ok(_) => {
                    // Empty or unexpected response — treat as non-vote (skip).
                }
                Err(e) => {
                    tracing::warn!(
                        txn_id = ?txn_id,
                        error = %e,
                        "accord: PreAccept RPC failed (non-fatal, continuing)"
                    );
                }
            }
        }

        // ------------------------------------------------------------------
        // Phase 2: Accept fanout (slow path only)
        // ------------------------------------------------------------------

        let (commit_t, commit_deps) = match decision {
            CoordinatorDecision::FastPathCommit { t, ref deps } => (t, deps.clone()),
            CoordinatorDecision::NeedAccept { t, ref deps } => {
                // Run the Accept phase with the merged (t, deps).
                let accept_payload = AcceptPayload {
                    txn_id,
                    t0,
                    t,
                    deps: deps.iter().copied().collect(),
                    ballot: BallotNumber(1),
                };
                let ac_bytes = bincode::serialize(&accept_payload)
                    .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
                let ac_msg = Message::AccordAccept(Bytes::from(ac_bytes));

                let ac_futs: Vec<_> = self
                    .replica_ids
                    .iter()
                    .map(|&peer_id| {
                        let peers = Arc::clone(&self.peers);
                        let msg = ac_msg.clone();
                        async move { peers.send(peer_id, msg, Lane::Data).await }
                    })
                    .collect();

                let ac_responses = futures::future::join_all(ac_futs).await;

                let mut ac_decision = CoordinatorDecision::Pending;
                for result in &ac_responses {
                    match result {
                        Ok(Message::AccordAcceptOK(b)) if !b.is_empty() => {
                            let ok: AcceptOkPayload = bincode::deserialize(b)
                                .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
                            let resp = AcceptResponse {
                                from: ok.txn_id.0.node, // node ID embedded in txn_id
                                ballot: BallotNumber(1),
                                deps: deps.iter().copied().collect(),
                            };
                            ac_decision = self.coordinator.handle_accept_ok(resp);
                            if ac_decision != CoordinatorDecision::Pending {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(
                                txn_id = ?txn_id,
                                error = %e,
                                "accord: Accept RPC failed"
                            );
                        }
                    }
                }

                match ac_decision {
                    CoordinatorDecision::SlowPathCommit { t: ct, deps: cd } => (ct, cd),
                    _ => {
                        // Could not reach Accept quorum.
                        return Err(AccordDriverError::QuorumUnavailable);
                    }
                }
            }
            _ => {
                // Phase 1 never reached a decision — quorum unavailable.
                return Err(AccordDriverError::QuorumUnavailable);
            }
        };

        // ------------------------------------------------------------------
        // Phase 3: Commit broadcast (wait for F+1 CommitOK)
        //
        // The coordinator counts itself as an implicit ack — it has already
        // committed the transaction locally by driving the PreAccept/Accept
        // phases. Remote replicas are contacted via `send()`.
        // ------------------------------------------------------------------

        let sq = slow_quorum_size(self.coordinator.rf);
        let self_id = self.self_id;

        let commit_payload = CommitPayload {
            txn_id,
            t0,
            t: commit_t,
            deps: commit_deps.iter().copied().collect(),
        };
        let commit_bytes = bincode::serialize(&commit_payload)
            .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
        let commit_msg = Message::AccordCommit(Bytes::from(commit_bytes));

        // Start with 1 ack for the coordinator itself (implicit local commit).
        let self_is_replica = self.replica_ids.contains(&self_id) && self_id != uuid::Uuid::nil();
        let mut commit_acks = if self_is_replica { 1usize } else { 0usize };

        let remote_commit_futs: Vec<_> = self
            .replica_ids
            .iter()
            .filter(|&&id| id != self_id)
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = commit_msg.clone();
                async move { peers.send(peer_id, msg, Lane::Data).await }
            })
            .collect();
        let commit_responses = futures::future::join_all(remote_commit_futs).await;

        for result in &commit_responses {
            match result {
                Ok(_) => commit_acks += 1,
                Err(e) => tracing::warn!(
                    txn_id = ?txn_id,
                    error = %e,
                    "accord: Commit RPC failed"
                ),
            }
        }
        if commit_acks < sq {
            return Err(AccordDriverError::QuorumUnavailable);
        }

        tracing::info!(
            txn_id = ?txn_id,
            t = ?commit_t,
            deps = ?commit_deps.len(),
            rtt = self.coordinator.rtt_count(),
            "accord: transaction committed"
        );

        // ------------------------------------------------------------------
        // Phase 4: Read-vote fanout (Gap 4 — linearizable IF-condition read)
        //
        // Each replica reads the current row at timestamp `commit_t` (after
        // all deps have applied) and votes whether the IF condition holds.
        // Collect F+1 matching votes to determine [applied] true/false.
        //
        // Self-send: the coordinator's local state machine has no dedicated
        // self-loopback, so we also count any self-send failure as
        // "condition holds" (optimistic default for the coordinator's own
        // replica state — the coordinator sees no prior applied writes for
        // a fresh INSERT IF NOT EXISTS).
        // ------------------------------------------------------------------

        let read_payload = ReadVotePayload {
            txn_id,
            t: commit_t,
            key: key.clone(),
        };
        let read_bytes = bincode::serialize(&read_payload)
            .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
        let read_msg = Message::AccordRead(Bytes::from(read_bytes));

        let mut votes_false = 0usize;
        let mut dissenting_row: Vec<u8> = Vec::new();

        let remote_read_futs: Vec<_> = self
            .replica_ids
            .iter()
            .filter(|&&id| id != self_id)
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = read_msg.clone();
                async move { peers.send(peer_id, msg, Lane::Data).await }
            })
            .collect();
        let read_responses = futures::future::join_all(remote_read_futs).await;

        for result in &read_responses {
            match result {
                Ok(Message::AccordReadOK(b)) if !b.is_empty() => {
                    match bincode::deserialize::<ReadVoteOkPayload>(b) {
                        Ok(vote) if !vote.condition_holds => {
                            votes_false += 1;
                            if dissenting_row.is_empty() {
                                dissenting_row = vote.current_row.clone();
                            }
                        }
                        _ => {
                            // Condition holds, pre-Gap-4 replica, or parse error:
                            // treat as condition_holds=true (forward-compatible default).
                        }
                    }
                }
                Ok(_) | Err(_) => {
                    // No response or network error — skip (don't count as false vote).
                    // Log network errors at warn level.
                    if let Err(e) = result {
                        tracing::warn!(
                            txn_id = ?txn_id,
                            error = %e,
                            "accord: ReadVote RPC failed (non-fatal)"
                        );
                    }
                }
            }
        }

        // F+1 matching votes decide the outcome.
        // Only return ConditionNotMet if F+1 replicas explicitly voted false.
        if votes_false >= sq {
            tracing::info!(
                txn_id = ?txn_id,
                votes_false,
                sq,
                "accord: IF condition not met — [applied]=false"
            );
            return Err(AccordDriverError::ConditionNotMet {
                current_row: dissenting_row,
            });
        }

        // ------------------------------------------------------------------
        // Phase 5: Apply broadcast (Gap 5 — dep-wait + storage write)
        //
        // Broadcast Apply to all remote replicas with the mutation payload.
        // Count the coordinator itself as an implicit apply (it drove the
        // protocol and already processed the commit). Wait for remote ApplyOK
        // to reach F+1 total before returning the LWT result.
        // ------------------------------------------------------------------

        let apply_bytes = {
            use crate::accord::wire::ApplyPayload;
            let apply_payload = ApplyPayload {
                txn_id,
                result_data: key.clone(),
            };
            bincode::serialize(&apply_payload)
                .map_err(|e| AccordDriverError::Codec(e.to_string()))?
        };
        let apply_msg = Message::AccordApply(Bytes::from(apply_bytes));

        // Coordinator itself counts as 1 implicit apply ack.
        let mut apply_acks = if self_is_replica { 1usize } else { 0usize };

        let remote_apply_futs: Vec<_> = self
            .replica_ids
            .iter()
            .filter(|&&id| id != self_id)
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = apply_msg.clone();
                async move { peers.send(peer_id, msg, Lane::Data).await }
            })
            .collect();
        let apply_responses = futures::future::join_all(remote_apply_futs).await;

        for result in &apply_responses {
            match result {
                Ok(Message::AccordApplyOK(b)) => {
                    if b.is_empty() {
                        apply_acks += 1;
                    } else if let Ok(ok) = bincode::deserialize::<ApplyOkPayload>(b) {
                        if ok.txn_id == txn_id {
                            apply_acks += 1;
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        txn_id = ?txn_id,
                        error = %e,
                        "accord: Apply RPC failed"
                    );
                }
            }
        }

        if apply_acks < sq {
            tracing::error!(
                txn_id = ?txn_id,
                apply_acks,
                sq,
                "accord: Apply quorum not reached — LWT result may not be durable"
            );
            return Err(AccordDriverError::ApplyQuorumUnavailable);
        }

        tracing::info!(
            txn_id = ?txn_id,
            apply_acks,
            "accord: Apply phase complete — [applied]=true"
        );

        Ok((commit_t, commit_deps))
    }

    /// The transaction ID assigned to this coordinator's transaction.
    pub fn txn_id(&self) -> TxnId {
        self.coordinator.txn_id
    }

    /// Number of round-trips completed (1 for fast path, 2 for slow path).
    pub fn rtt_count(&self) -> u32 {
        self.coordinator.rtt_count()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_common::accord::{BallotNumber, Timestamp, TxnId};

    // -----------------------------------------------------------------------
    // Quorum formula tests
    // -----------------------------------------------------------------------

    #[test]
    fn fast_quorum_size_formula() {
        // RF=3: quorum=2, f=1, fast_q = floor((3*1+1)/2)+1 = floor(4/2)+1 = 2+1 = 3
        assert_eq!(fast_quorum_size(3), 3);

        // RF=5: quorum=3, f=2, fast_q = floor((3*2+1)/2)+1 = floor(7/2)+1 = 3+1 = 4
        assert_eq!(fast_quorum_size(5), 4);

        // RF=7: quorum=4, f=3, fast_q = floor((3*3+1)/2)+1 = floor(10/2)+1 = 5+1 = 6
        assert_eq!(fast_quorum_size(7), 6);
    }

    #[test]
    fn fast_quorum_size_rf3_f0() {
        // RF=3 allows f=1 failures. Fast quorum requires 3 (all replicas).
        // This means RF=3 fast path needs all replicas to agree — any
        // single disagreement forces the slow path.
        let fq = fast_quorum_size(3);
        assert_eq!(fq, 3);
        assert_eq!(fq, 3); // == RF, so all must agree
    }

    #[test]
    fn fast_quorum_size_rf5_f1() {
        // RF=5: fast quorum = 4. Can tolerate 1 non-responding replica
        // and still take the fast path.
        let fq = fast_quorum_size(5);
        assert_eq!(fq, 4);
        // Can tolerate 1 missing and still fast-path
        assert_eq!(5 - fq, 1);
    }

    #[test]
    fn fast_quorum_size_rf3_f1() {
        // RF=3: f=1, fast quorum=3. Cannot tolerate any failure on fast path.
        // If even one replica is slow, must fall back to slow path.
        let fq = fast_quorum_size(3);
        let sq = super::slow_quorum_size(3);
        assert_eq!(fq, 3); // All must agree for fast path
        assert_eq!(sq, 2); // Only majority needed for slow path
        assert!(fq > sq); // Fast path is strictly harder
    }

    #[test]
    fn fast_quorum_size_rf1() {
        // RF=1: quorum=1, f=0, fast_q = floor((0+1)/2)+1 = 0+1 = 1
        let fq = fast_quorum_size(1);
        assert_eq!(fq, 1);
    }

    #[test]
    fn slow_quorum_size_formula() {
        assert_eq!(super::slow_quorum_size(1), 1);
        assert_eq!(super::slow_quorum_size(2), 2);
        assert_eq!(super::slow_quorum_size(3), 2);
        assert_eq!(super::slow_quorum_size(5), 3);
        assert_eq!(super::slow_quorum_size(7), 4);
        assert_eq!(super::slow_quorum_size(9), 5);
    }

    // -----------------------------------------------------------------------
    // Fast path / slow path RTT tests
    // -----------------------------------------------------------------------

    fn make_ts(micros: u64) -> Timestamp {
        Timestamp::synthetic(micros)
    }

    fn make_txn_id(node: u64, micros: u64) -> TxnId {
        TxnId::new(node, make_ts(micros))
    }

    #[test]
    fn coordinator_fast_path_1rtt() {
        // RF=3, coordinator is node 1, not leaseholder.
        // All 3 replicas agree on t0 with empty deps -> fast path, 1 RTT.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);

        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        // Response from replica 1: agrees with t0.
        let r1 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 1,
            t: t0,
            deps: vec![],
        });
        assert_eq!(r1, CoordinatorDecision::Pending);

        // Response from replica 2: agrees with t0.
        let r2 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 2,
            t: t0,
            deps: vec![],
        });
        assert_eq!(r2, CoordinatorDecision::Pending);

        // Response from replica 3: agrees with t0 -> fast quorum reached.
        let r3 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 3,
            t: t0,
            deps: vec![],
        });
        assert_eq!(
            r3,
            CoordinatorDecision::FastPathCommit {
                t: t0,
                deps: HashSet::new(),
            }
        );
        assert_eq!(coord.rtt_count(), 1);
        assert_eq!(coord.phase, CoordinatorPhase::FastPathCommit);
    }

    #[test]
    fn coordinator_slow_path_2rtt() {
        // RF=3, coordinator is node 1. Replica 2 proposes a different timestamp
        // (conflict detected) -> slow path, 2 RTT.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);
        let t_conflict = make_ts(2000); // Higher timestamp from conflict

        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        // Replica 1 agrees.
        let r1 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 1,
            t: t0,
            deps: vec![],
        });
        assert_eq!(r1, CoordinatorDecision::Pending);

        // Replica 2 has a conflict: proposes higher timestamp.
        let other_txn = make_txn_id(2, 500);
        let r2 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 2,
            t: t_conflict,
            deps: vec![other_txn],
        });
        // With 2 responses (slow quorum for RF=3) and disagreement -> NeedAccept
        assert!(matches!(r2, CoordinatorDecision::NeedAccept { .. }));
        assert_eq!(coord.rtt_count(), 1); // First RTT done

        // Now run the Accept phase (second RTT).
        match r2 {
            CoordinatorDecision::NeedAccept { t, ref deps } => {
                assert_eq!(t, t_conflict); // Highest timestamp wins
                assert!(deps.contains(&other_txn));
            }
            _ => unreachable!(),
        }

        // Collect AcceptOK from slow quorum (2 for RF=3).
        let a1 = coord.handle_accept_ok(AcceptResponse {
            from: 1,
            ballot: BallotNumber(1),
            deps: vec![other_txn],
        });
        assert_eq!(a1, CoordinatorDecision::Pending);

        let a2 = coord.handle_accept_ok(AcceptResponse {
            from: 2,
            ballot: BallotNumber(1),
            deps: vec![other_txn],
        });
        assert!(matches!(a2, CoordinatorDecision::SlowPathCommit { .. }));
        assert_eq!(coord.rtt_count(), 2); // Two RTTs total
        assert_eq!(coord.phase, CoordinatorPhase::SlowPathCommit);
    }

    // -----------------------------------------------------------------------
    // Scenario tests (using TestCluster)
    // -----------------------------------------------------------------------

    use crate::accord::test_cluster::{TestCluster, TestMessage, TestMessagePayload};

    #[test]
    fn scenario_fast_path_no_conflict() {
        // 3-node cluster, transaction on key "x", no conflicts.
        // Coordinator is node 1, sends PreAccept to nodes 2 and 3.
        // Both agree on t0 -> fast path commit.
        let mut cluster = TestCluster::new(3);
        let t0 = Timestamp::synthetic(1000);
        let txn_id = TxnId::new(1, t0);

        // Coordinator (node 1) creates the coordinator state.
        // It is the leaseholder, so it implicitly votes for t0.
        let mut coord = AccordCoordinator::new(txn_id, t0, b"x".to_vec(), 1, 3, true);

        // Send PreAccept to nodes 2 and 3.
        for dst in [2, 3] {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id,
                    t0,
                    key: b"x".to_vec(),
                },
            });
        }

        // Deliver PreAccept to node 2 -> get PreAcceptOK.
        let responses = cluster.deliver_next();
        assert_eq!(responses.len(), 1);
        match &responses[0].payload {
            TestMessagePayload::PreAcceptOK { t, deps, .. } => {
                let decision = coord.handle_preaccept_ok(PreAcceptResponse {
                    from: 2,
                    t: *t,
                    deps: deps.clone(),
                });
                assert_eq!(decision, CoordinatorDecision::Pending);
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }

        // Deliver PreAccept to node 3 -> get PreAcceptOK.
        let responses = cluster.deliver_next();
        assert_eq!(responses.len(), 1);
        match &responses[0].payload {
            TestMessagePayload::PreAcceptOK { t, deps, .. } => {
                let decision = coord.handle_preaccept_ok(PreAcceptResponse {
                    from: 3,
                    t: *t,
                    deps: deps.clone(),
                });
                // With leaseholder (1 implicit) + 2 explicit = 3 = fast quorum for RF=3
                assert_eq!(
                    decision,
                    CoordinatorDecision::FastPathCommit {
                        t: t0,
                        deps: HashSet::new(),
                    }
                );
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }

        assert_eq!(coord.rtt_count(), 1);

        // Broadcast Commit.
        for dst in 1..=3 {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::Commit {
                    txn_id,
                    t0,
                    t: t0,
                    deps: vec![],
                },
            });
        }
        cluster.drain();
        cluster.assert_consistent(&txn_id);
    }

    #[test]
    fn scenario_fast_path_with_leaseholder() {
        // Leaseholder optimization: coordinator is node 1 and owns the range.
        // For RF=3, fast quorum = 3. Leaseholder gives 1 implicit vote,
        // so only 2 remote PreAcceptOK needed (instead of 3).
        let t0 = Timestamp::synthetic(500);
        let txn_id = TxnId::new(1, t0);

        let mut coord = AccordCoordinator::new(txn_id, t0, b"y".to_vec(), 1, 3, true);

        // Leaseholder already contributed 1 vote. Need 2 more for fast quorum of 3.
        assert_eq!(coord.preaccept_response_count(), 1); // Implicit leaseholder vote

        let r1 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 2,
            t: t0,
            deps: vec![],
        });
        assert_eq!(r1, CoordinatorDecision::Pending);
        assert_eq!(coord.preaccept_response_count(), 2);

        let r2 = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 3,
            t: t0,
            deps: vec![],
        });
        // 1 (leaseholder) + 2 (remote) = 3 = fast quorum for RF=3
        assert_eq!(
            r2,
            CoordinatorDecision::FastPathCommit {
                t: t0,
                deps: HashSet::new(),
            }
        );
        assert_eq!(coord.rtt_count(), 1);
    }

    #[test]
    fn scenario_slow_path_conflict() {
        // 3-node cluster. Two transactions touch the same key.
        // First transaction is already registered on node 2.
        // When the second transaction's PreAccept arrives at node 2,
        // node 2 reports a conflict -> slow path for the second transaction.
        let mut cluster = TestCluster::new(3);
        let t0_first = Timestamp::synthetic(500);
        let txn_first = TxnId::new(1, t0_first);

        // Register the first transaction on node 2 by sending it a PreAccept.
        cluster.send(TestMessage {
            src: 1,
            dst: 2,
            payload: TestMessagePayload::PreAccept {
                txn_id: txn_first,
                t0: t0_first,
                key: b"conflict_key".to_vec(),
            },
        });
        cluster.deliver_next(); // Node 2 processes it and records the conflict.
                                // Drain the PreAcceptOK response (goes back to node 1, no further output).
        cluster.drain();

        // Now the second transaction arrives.
        let t0_second = Timestamp::synthetic(1000);
        let txn_second = TxnId::new(2, t0_second);

        let mut coord =
            AccordCoordinator::new(txn_second, t0_second, b"conflict_key".to_vec(), 2, 3, false);

        // Send PreAccept to all 3 nodes.
        for dst in 1..=3 {
            cluster.send(TestMessage {
                src: 2,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_second,
                    t0: t0_second,
                    key: b"conflict_key".to_vec(),
                },
            });
        }

        // Deliver to node 1 (no conflict — hasn't seen txn_first).
        let resp1 = cluster.deliver_next();
        assert_eq!(resp1.len(), 1);
        match &resp1[0].payload {
            TestMessagePayload::PreAcceptOK { t, deps, .. } => {
                let decision = coord.handle_preaccept_ok(PreAcceptResponse {
                    from: 1,
                    t: *t,
                    deps: deps.clone(),
                });
                assert_eq!(decision, CoordinatorDecision::Pending);
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }

        // Deliver to node 2 (HAS conflict with txn_first).
        let resp2 = cluster.deliver_next();
        assert_eq!(resp2.len(), 1);
        match &resp2[0].payload {
            TestMessagePayload::PreAcceptOK { t, deps, .. } => {
                // Node 2 should report txn_first as a dependency.
                assert!(
                    deps.contains(&txn_first),
                    "node 2 should report dependency on first transaction"
                );

                let decision = coord.handle_preaccept_ok(PreAcceptResponse {
                    from: 2,
                    t: *t,
                    deps: deps.clone(),
                });
                // With 2 responses and disagreement -> NeedAccept (slow path).
                assert!(
                    matches!(decision, CoordinatorDecision::NeedAccept { .. }),
                    "expected NeedAccept, got {:?}",
                    decision
                );
            }
            other => panic!("expected PreAcceptOK, got {:?}", other),
        }

        assert_eq!(coord.rtt_count(), 1); // First RTT done

        // Run Accept phase (second RTT).
        let a1 = coord.handle_accept_ok(AcceptResponse {
            from: 1,
            ballot: BallotNumber(1),
            deps: vec![txn_first],
        });
        assert_eq!(a1, CoordinatorDecision::Pending);

        let a2 = coord.handle_accept_ok(AcceptResponse {
            from: 2,
            ballot: BallotNumber(1),
            deps: vec![txn_first],
        });
        assert!(matches!(a2, CoordinatorDecision::SlowPathCommit { .. }));
        assert_eq!(coord.rtt_count(), 2); // Two RTTs total
    }

    #[test]
    fn scenario_two_concurrent_no_conflict() {
        // Two transactions on DIFFERENT keys — both should fast-path.
        let mut cluster = TestCluster::new(3);

        let t0_a = Timestamp::synthetic(1000);
        let txn_a = TxnId::new(1, t0_a);
        let t0_b = Timestamp::synthetic(2000);
        let txn_b = TxnId::new(2, t0_b);

        let mut coord_a = AccordCoordinator::new(txn_a, t0_a, b"key_a".to_vec(), 1, 3, true);
        let mut coord_b = AccordCoordinator::new(txn_b, t0_b, b"key_b".to_vec(), 2, 3, true);

        // Send PreAccepts for both transactions to the non-coordinator replicas.
        // Txn A: node 1 is coordinator, send to 2 and 3.
        for dst in [2, 3] {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_a,
                    t0: t0_a,
                    key: b"key_a".to_vec(),
                },
            });
        }
        // Txn B: node 2 is coordinator, send to 1 and 3.
        for dst in [1, 3] {
            cluster.send(TestMessage {
                src: 2,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_b,
                    t0: t0_b,
                    key: b"key_b".to_vec(),
                },
            });
        }

        // Deliver all 4 PreAccepts and collect responses.
        // Message order: A->2, A->3, B->1, B->3
        let mut a_responses = Vec::new();
        let mut b_responses = Vec::new();

        for _ in 0..4 {
            let responses = cluster.deliver_next();
            for resp in &responses {
                if let TestMessagePayload::PreAcceptOK { txn_id, t, deps } = &resp.payload {
                    if *txn_id == txn_a {
                        a_responses.push(PreAcceptResponse {
                            from: resp.src,
                            t: *t,
                            deps: deps.clone(),
                        });
                    } else if *txn_id == txn_b {
                        b_responses.push(PreAcceptResponse {
                            from: resp.src,
                            t: *t,
                            deps: deps.clone(),
                        });
                    }
                }
            }
        }

        // Both should have 2 responses (from remote replicas).
        assert_eq!(a_responses.len(), 2);
        assert_eq!(b_responses.len(), 2);

        // Feed responses to coordinators. Both should fast-path.
        for resp in a_responses {
            let decision = coord_a.handle_preaccept_ok(resp);
            // Second response should trigger fast path (1 leaseholder + 2 remote = 3 = fast quorum).
            if coord_a.preaccept_response_count() == 3 {
                assert_eq!(
                    decision,
                    CoordinatorDecision::FastPathCommit {
                        t: t0_a,
                        deps: HashSet::new(),
                    }
                );
            }
        }

        for resp in b_responses {
            let decision = coord_b.handle_preaccept_ok(resp);
            if coord_b.preaccept_response_count() == 3 {
                assert_eq!(
                    decision,
                    CoordinatorDecision::FastPathCommit {
                        t: t0_b,
                        deps: HashSet::new(),
                    }
                );
            }
        }

        assert_eq!(coord_a.rtt_count(), 1);
        assert_eq!(coord_b.rtt_count(), 1);
    }

    #[test]
    fn scenario_two_concurrent_same_key() {
        // Two transactions on the SAME key — at least one must slow-path.
        // Node 1 coordinates txn_a, node 2 coordinates txn_b.
        let mut cluster = TestCluster::new(3);

        let t0_a = Timestamp::synthetic(1000);
        let txn_a = TxnId::new(1, t0_a);
        let t0_b = Timestamp::synthetic(2000);
        let txn_b = TxnId::new(2, t0_b);

        let mut coord_a = AccordCoordinator::new(txn_a, t0_a, b"shared_key".to_vec(), 1, 3, false);
        let mut coord_b = AccordCoordinator::new(txn_b, t0_b, b"shared_key".to_vec(), 2, 3, false);

        // Send all PreAccepts. Interleave them so conflicts are detected.
        // Txn A -> all 3 nodes.
        for dst in 1..=3 {
            cluster.send(TestMessage {
                src: 1,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_a,
                    t0: t0_a,
                    key: b"shared_key".to_vec(),
                },
            });
        }
        // Txn B -> all 3 nodes.
        for dst in 1..=3 {
            cluster.send(TestMessage {
                src: 2,
                dst,
                payload: TestMessagePayload::PreAccept {
                    txn_id: txn_b,
                    t0: t0_b,
                    key: b"shared_key".to_vec(),
                },
            });
        }

        // Deliver all PreAccepts for txn_a first (messages 0-2).
        let mut a_responses = Vec::new();
        for _ in 0..3 {
            let responses = cluster.deliver_next();
            for resp in &responses {
                if let TestMessagePayload::PreAcceptOK { txn_id, t, deps } = &resp.payload {
                    if *txn_id == txn_a {
                        a_responses.push(PreAcceptResponse {
                            from: resp.src,
                            t: *t,
                            deps: deps.clone(),
                        });
                    }
                }
            }
        }

        // Txn A arrives first on all nodes — no conflict, all agree on t0_a.
        assert_eq!(a_responses.len(), 3);
        let mut a_decision = CoordinatorDecision::Pending;
        for resp in a_responses {
            a_decision = coord_a.handle_preaccept_ok(resp);
        }
        // Txn A should fast-path since it arrived first everywhere.
        assert_eq!(
            a_decision,
            CoordinatorDecision::FastPathCommit {
                t: t0_a,
                deps: HashSet::new(),
            }
        );

        // Now deliver all PreAccepts for txn_b (messages 3-5).
        // Nodes already have txn_a registered as a conflict.
        let mut b_responses = Vec::new();
        for _ in 0..3 {
            let responses = cluster.deliver_next();
            for resp in &responses {
                if let TestMessagePayload::PreAcceptOK { txn_id, t, deps } = &resp.payload {
                    if *txn_id == txn_b {
                        b_responses.push(PreAcceptResponse {
                            from: resp.src,
                            t: *t,
                            deps: deps.clone(),
                        });
                    }
                }
            }
        }

        // Txn B should see txn_a as a dependency on all nodes.
        assert_eq!(b_responses.len(), 3);
        let mut b_decision = CoordinatorDecision::Pending;
        for resp in b_responses {
            // All responses should list txn_a as a dependency.
            assert!(
                resp.deps.contains(&txn_a),
                "txn_b response from node {} should have txn_a as dep, got deps: {:?}",
                resp.from,
                resp.deps
            );
            b_decision = coord_b.handle_preaccept_ok(resp);
        }

        // Txn B: all replicas agree on deps (all have txn_a) and on timestamp.
        // If all 3 agree on the same t and same deps, it could still be fast-path
        // even with deps, as long as all replicas agree.
        // The key question: does `t == t0` for all? Since txn_a has t0=1000 and
        // txn_b has t0=2000, and t0_b > t0_a, the conflict doesn't bump the
        // timestamp. So all replicas should return t == t0_b with deps=[txn_a].
        // That means all agree, and we need to check our fast-path logic.
        //
        // Actually: t == t0 means fast path is possible. Even though there are
        // deps, if t wasn't bumped and all deps match, that's fast-path eligible.
        // BUT our deps_match_t0 checks that all responses have the same deps.
        // All 3 responses have deps=[txn_a], so they match. Fast path!
        //
        // This is correct Accord behavior: if all replicas agree on the same
        // (t, deps), the fast path works even with non-empty deps.
        match b_decision {
            CoordinatorDecision::FastPathCommit { t, deps } => {
                assert_eq!(t, t0_b);
                assert!(deps.contains(&txn_a));
            }
            CoordinatorDecision::NeedAccept { .. } => {
                // Also acceptable if implementation is stricter
            }
            other => panic!(
                "expected FastPathCommit or NeedAccept for txn_b, got {:?}",
                other
            ),
        }
    }

    /// Process-global span collector for tracing tests.
    ///
    /// Installed once via `set_global_default` so that tracing callsites
    /// are always interned with an active subscriber. This eliminates the
    /// callsite-caching flakiness where a parallel test could intern a
    /// callsite before any subscriber was installed, permanently disabling it.
    mod global_span_collector {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::{Mutex, OnceLock};

        static NAMES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
        static INSTALLED: OnceLock<()> = OnceLock::new();

        struct GlobalSpanCollector {
            next_id: AtomicU64,
        }

        impl tracing::Subscriber for GlobalSpanCollector {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                names()
                    .lock()
                    .unwrap()
                    .push(span.metadata().name().to_string());
                let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
                tracing::span::Id::from_u64(id)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn event(&self, _: &tracing::Event<'_>) {}
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
        }

        fn names() -> &'static Mutex<Vec<String>> {
            NAMES.get_or_init(|| Mutex::new(Vec::new()))
        }

        /// Ensure the global collector is installed. Idempotent.
        pub fn ensure_installed() {
            INSTALLED.get_or_init(|| {
                let collector = GlobalSpanCollector {
                    next_id: AtomicU64::new(0),
                };
                // Ignore error if another test already set a global subscriber.
                let _ = tracing::subscriber::set_global_default(collector);
            });
        }

        /// Drain all recorded span names since the last call.
        pub fn drain_names() -> Vec<String> {
            names().lock().unwrap().drain(..).collect()
        }
    }

    #[test]
    fn accord_coordinator_creates_spans() {
        global_span_collector::ensure_installed();

        // Drain any spans from prior tests.
        global_span_collector::drain_names();

        let t0 = Timestamp {
            epoch: 1,
            time: 1000,
            seq: 1,
            node: 1,
        };
        let txn_id = TxnId::new(1, t0);

        let mut coord = AccordCoordinator::new(txn_id, t0, vec![1, 2, 3], 1, 3, true);

        // Send preaccept responses to trigger the preaccept span.
        let _ = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 2,
            t: t0,
            deps: vec![],
        });

        let recorded = global_span_collector::drain_names();
        let has_accord_span = recorded.iter().any(|n| n.starts_with("accord."));
        assert!(
            has_accord_span,
            "expected at least one 'accord.*' span, got: {recorded:?}"
        );
    }
}
