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

use crate::accord::transport::AccordTransport;
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

    /// Conclude the PreAccept phase when no further response can arrive.
    ///
    /// `handle_preaccept_ok` deliberately stays `Pending` while a fast quorum
    /// is still reachable: with RF=3 the fast path needs all three replicas,
    /// so two agreeing votes must keep waiting rather than spend a second RTT
    /// on the Accept phase. That is correct *while responses are outstanding*.
    ///
    /// Once the fan-out is exhausted the fast path is provably unreachable.
    /// If a slow quorum answered, the round can and must commit through the
    /// Accept phase. Without this the coordinator sat at `Pending` forever and
    /// the caller reported "Accord quorum unavailable" on a fully healthy
    /// cluster — every LWT on a non-leaseholder RF=3 coordinator that lost a
    /// single vote failed, even though two replicas had agreed.
    ///
    /// Returns `Pending` when fewer than a slow quorum responded (the round is
    /// genuinely unavailable and the caller must fail loud) or when a decision
    /// was already reached, in which case this is a no-op.
    pub fn finalize_preaccept(&mut self) -> CoordinatorDecision {
        if self.phase != CoordinatorPhase::PreAccepting {
            return CoordinatorDecision::Pending;
        }

        if self.preaccept_responses.len() < slow_quorum_size(self.rf) {
            return CoordinatorDecision::Pending;
        }

        self.phase = CoordinatorPhase::Accepting;
        self.rtt_count = 1;
        CoordinatorDecision::NeedAccept {
            t: self.merged_t,
            deps: self.merged_deps.clone(),
        }
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

/// Generic-`IF` condition gate: given the F+1-agreed row bytes at `t`
/// (`None` if the row was absent), returns `true` iff the IF predicate holds
/// (the write should apply). Injected by the CQL router; see
/// [`AccordCoordinatorDriver::with_condition_gate`].
pub type ConditionGate = Box<dyn Fn(Option<&[u8]>) -> bool + Send + Sync>;

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
    /// Network seam: `PeerManager` in production, a mock in tests.
    peers: Arc<dyn AccordTransport>,
    /// IDs of the replicas for this transaction's token range.
    replica_ids: Vec<uuid::Uuid>,
    /// UUID of this coordinator node (used to identify self-sends).
    ///
    /// When `PeerManager::send` is called with this ID, the send will fail
    /// because the node is not registered in its own peer map. We treat the
    /// coordinator itself as an implicit ack for Commit and Apply (the
    /// coordinator drove the protocol and counts as one replica).
    self_id: uuid::Uuid,
    /// Encoded mutation to apply on commit: a self-describing commit-log
    /// `Mutation` (keyspace/table, `DecoratedKey`, rows, timestamp).
    ///
    /// This is what each replica decodes and writes to storage in the Apply
    /// phase — it is carried as the Apply payload's `result_data`. It is
    /// distinct from `coordinator.key` (the raw partition-key bytes used only
    /// for Accord conflict ordering). An empty vector means "no mutation"
    /// (read-only / protocol-only transactions).
    mutation: Vec<u8>,
    /// The transaction's full write-set: one `(key, mutation)` entry per
    /// partition written. A single-key transaction (the [`Self::new`] path) has
    /// exactly one entry whose `mutation` equals [`Self::mutation`] above; a
    /// multi-key transaction ([`Self::new_multi`]) has several. The Apply phase
    /// fans a per-replica [`Message::AccordApplyV2`](ferrosa_net::Message) (scoped
    /// to each replica's owned keys via [`Self::with_per_key_replicas`]) over this
    /// set, and the coordinator's own-replica Apply persists the keys it owns.
    write_set: Vec<crate::accord::wire::WriteSetEntry>,
    /// How replicas should answer the Gap-4 read-vote: existence semantics for
    /// `INSERT IF NOT EXISTS` (the default) or a generic read-row-at-`t` whose
    /// IF predicate this coordinator evaluates after collecting F+1 agreed rows.
    read_predicate: crate::accord::wire::ReadPredicate,
    /// For a generic `IF` read-vote: the row bytes that F+1 replicas agreed on at
    /// `t`, captured during the read phase so the router can decode and evaluate
    /// the predicate. `None` when the read-vote returned no row (row absent at
    /// `t`) or for the existence path. Read via [`Self::last_read_row`].
    last_read_row: Option<Vec<u8>>,
    /// Optional reader for the coordinator's OWN replica, used by the generic
    /// `IF` read-vote so the coordinator's local read-at-`t` counts toward F+1
    /// agreement (its self-send is not reachable over the network). Set via
    /// [`Self::with_local_reader`]; `None` falls back to remote votes only.
    local_reader: Option<Arc<dyn crate::accord::apply::StorageReader>>,
    /// Optional applier for the coordinator's OWN replica. The coordinator's
    /// self-send Apply RPC is unreachable, so without this its own node never
    /// persists the mutations it coordinates (a silent data-loss / read-skew
    /// hazard, and it makes the coordinator's local generic-`IF` read disagree
    /// with the replicas that did apply). When set, the coordinator applies the
    /// committed mutation locally during the Apply phase. Set via
    /// [`Self::with_local_applier`].
    local_applier: Option<Arc<dyn crate::accord::apply::StorageApplier>>,
    /// Optional handle to the coordinator's OWN replica state machine.
    ///
    /// The coordinator's self-send is unreachable over the network, so its local
    /// read-vote cannot go through the inbound `AccordRead` handler. When this is
    /// wired, the coordinator's local generic-`IF` read-at-`t` performs the SAME
    /// dependency-wait the remote handler does — blocking until every conflicting
    /// transaction `t0 < t` known to the local state machine has reached
    /// `Applied` — so a genuinely concurrent contender's write is observed before
    /// the read. Without it, two concurrent `INSERT IF NOT EXISTS` could each read
    /// the key as absent before either applies and BOTH apply (a double-apply /
    /// lost update). `None` keeps the bare-reader behavior (no local dep-wait).
    local_accord_state: Option<crate::accord::handlers::AccordState>,
    /// For the generic-`IF` path: evaluates the IF predicate against the
    /// F+1-agreed row bytes at `t`. Returns `true` iff the write should apply.
    ///
    /// The CQL operators (`IfCondition`/`CqlValue`) live in `ferrosa-cql`, which
    /// depends on this crate, so the coordinator cannot evaluate them directly.
    /// The router injects this closure (wrapping the canonical
    /// `eval_if_conditions`); the coordinator calls it in the read-vote phase and
    /// ABORTS with [`AccordDriverError::ConditionNotMet`] BEFORE the Apply phase
    /// when it returns `false`. This is what GATES the write on the condition —
    /// without it the generic path would apply unconditionally (a lost-update /
    /// wrong-`[applied]` bug). `None` keeps a permissive default (apply) for
    /// callers that have no generic predicate. Set via
    /// [`Self::with_condition_gate`].
    condition_gate: Option<ConditionGate>,
    /// Per-key replica resolver for multi-shard fan-out (ADR-021). Given a
    /// partition key's raw bytes, returns the replica host-ids that own it.
    ///
    /// When set, the apply phase builds the participant set via
    /// [`ParticipantSet::from_per_key`](crate::accord::shard_quorum::ParticipantSet::from_per_key)
    /// and sends each replica a per-replica `AccordApplyV2` scoped to ONLY the
    /// keys it owns — so a replica never persists a key it is not a replica for.
    /// `None` keeps the single-shard default (every replica owns every key →
    /// each gets the full write-set), preserving the pre-multi-shard behavior
    /// exactly. Production wires the ring resolver
    /// (`WritePath::replicas_for_key`) here; set via
    /// [`Self::with_per_key_replicas`].
    #[allow(clippy::type_complexity)]
    per_key_replicas: Option<Arc<dyn Fn(&[u8]) -> Vec<uuid::Uuid> + Send + Sync>>,
}

/// Return the row bytes that at least `quorum` of `reads` agree on, if any.
///
/// All collected reads must be the SAME bytes for the read to be linearizable:
/// any divergence means replicas disagree on the row state at `t`, which is a
/// correctness failure. Returns `Some(bytes)` only when `reads.len() >= quorum`
/// and every read is identical; `None` otherwise (caller aborts).
/// Why a PreAccept fanout produced no vote from a replica.
///
/// The replica encodes every non-OK outcome as an EMPTY `AccordPreAcceptOK`:
/// a `Nack`, a state machine that could not persist, and any unexpected
/// response all arrive looking identical. The coordinator used to skip them
/// with a bare `Ok(_) => {}`, so a transaction that failed for want of votes
/// reported "quorum unavailable" and nothing else — which is how the 2026-06
/// FileSyncWriter failure (PreAccept could not persist, so no replica voted)
/// stayed undiagnosed.
///
/// Naming the outcomes does not make an empty response informative on its own,
/// but it makes the count visible: how many peers answered, how many voted,
/// and how many returned nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreAcceptOutcome {
    /// A real vote carrying the replica's `t` and deps.
    Vote,
    /// The peer answered, but not with a usable vote (empty or unexpected).
    NoVote,
    /// The RPC itself failed — the peer never answered.
    RpcFailed,
}

/// Classify one PreAccept fanout result.
///
/// Pure so the counting is testable without a cluster: the fanout itself is an
/// inline async block over live peers and cannot be unit tested directly.
pub fn classify_preaccept_response(result: Result<&Message, ()>) -> PreAcceptOutcome {
    match result {
        Ok(Message::AccordPreAcceptOK(b)) if !b.is_empty() => PreAcceptOutcome::Vote,
        Ok(_) => PreAcceptOutcome::NoVote,
        Err(()) => PreAcceptOutcome::RpcFailed,
    }
}

fn agreed_row(reads: &[Vec<u8>], quorum: usize) -> Option<Vec<u8>> {
    if quorum == 0 || reads.len() < quorum {
        return None;
    }

    // Count votes per distinct value and take one that reaches the quorum.
    //
    // This asked `reads.iter().all(|r| r == first)` -- unanimity, not F+1. On
    // RF=3 that let a single lagging replica veto a genuine 2-of-3 majority,
    // so every generic-IF LWT was refused as non-linearizable while being
    // perfectly decidable. Observed live on 2026-08-22:
    //
    //     generic IF read-vote lacked F+1 (2) agreement on the row at t
    //     (got 3 reads) -- refusing a non-linearizable LWT
    //
    // Three reads, F+1 of two, and still refused. The function's own doc and
    // that error message both said F+1; only the code said "all".
    //
    // At most one value can reach a quorum, because two disjoint groups of
    // `quorum` votes would require `2 * quorum > reads.len()` to overlap --
    // exactly the majority property the caller relies on for linearizability.
    // So the first value to reach it is the only one, and returning it is
    // unambiguous.
    let mut counts: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    for read in reads {
        let n = counts.entry(read.as_slice()).or_insert(0);
        *n += 1;
        if *n >= quorum {
            return Some(read.clone());
        }
    }
    None
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
    /// - `key`: raw partition key bytes (used for Accord conflict ordering).
    /// - `mutation`: encoded commit-log `Mutation` to apply on commit, carried
    ///   as the Apply payload's `result_data`. Empty for read-only txns.
    pub fn new(
        node_id: u64,
        replica_ids: Vec<uuid::Uuid>,
        peers: Arc<PeerManager>,
        is_leaseholder: bool,
        clock: &HybridLogicalClock,
        key: Vec<u8>,
        mutation: Vec<u8>,
    ) -> Self {
        // A single-key transaction is the degenerate one-entry write-set.
        Self::new_multi(
            node_id,
            replica_ids,
            peers,
            is_leaseholder,
            clock,
            vec![(key, mutation)],
        )
    }

    /// Build a driver for a multi-key (multi-partition) transaction.
    ///
    /// `write_set` is one `(partition_key, encoded_mutation)` per key the
    /// transaction writes; it must be non-empty. Conflict ordering unions
    /// dependencies across ALL keys via `AccordPreAcceptV2` (t_276e12); the first
    /// key is kept only as the representative for the single-key ReadVote and the
    /// v1 wire path. The Apply phase builds a
    /// per-shard participant ([`Self::with_per_key_replicas`]) and fans a
    /// per-replica `AccordApplyV2` (scoped to each replica's owned keys) out under
    /// per-shard quorum; the coordinator applies the keys it owns locally as one
    /// atomic write-set. Without a resolver this collapses to the single-shard
    /// case (every replica owns every key), so an RF=1 / coordinator-is-sole-
    /// replica multi-key transaction commits all keys atomically.
    pub fn new_multi(
        node_id: u64,
        replica_ids: Vec<uuid::Uuid>,
        peers: Arc<PeerManager>,
        is_leaseholder: bool,
        clock: &HybridLogicalClock,
        write_set: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Self {
        // Coerce the concrete PeerManager into the transport seam; all driver
        // logic is shared with the test-injectable `new_multi_with_transport`.
        let transport: Arc<dyn AccordTransport> = peers;
        Self::new_multi_with_transport(
            node_id,
            replica_ids,
            transport,
            is_leaseholder,
            clock,
            write_set,
        )
    }

    /// Like [`Self::new_multi`] but takes the [`AccordTransport`] seam directly,
    /// so tests can inject a mock that returns controllable per-node responses
    /// (exercising the multi-node Commit/Apply quorum logic without a network).
    pub(crate) fn new_multi_with_transport(
        node_id: u64,
        replica_ids: Vec<uuid::Uuid>,
        peers: Arc<dyn AccordTransport>,
        is_leaseholder: bool,
        clock: &HybridLogicalClock,
        write_set: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Self {
        let rf = replica_ids.len();
        assert!(rf > 0, "replica_ids must be non-empty");
        assert!(!write_set.is_empty(), "write_set must be non-empty");

        let t0 = clock.now();
        let txn_id = TxnId::new(node_id, t0);

        // Representative key for conflict ordering (the inner coordinator is
        // single-key for now); the per-key union is Phase 2.
        let key = write_set[0].0.clone();
        // Keep `mutation` = the first entry so the single-key wire path (PreAccept
        // key, v1 Apply payload) is byte-identical for a one-entry write-set.
        let mutation = write_set[0].1.clone();
        let write_set: Vec<crate::accord::wire::WriteSetEntry> = write_set
            .into_iter()
            .map(|(key, mutation)| crate::accord::wire::WriteSetEntry { key, mutation })
            .collect();

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
            mutation,
            write_set,
            read_predicate: crate::accord::wire::ReadPredicate::NotExists,
            last_read_row: None,
            local_reader: None,
            local_applier: None,
            local_accord_state: None,
            condition_gate: None,
            per_key_replicas: None,
        }
    }

    /// Supply an applier for the coordinator's own replica so it persists the
    /// mutations it coordinates (its self-send Apply RPC is unreachable).
    ///
    /// Without this the coordinator node silently lacks its own LWT writes; with
    /// it, the coordinator's storage matches the replicas' and its local
    /// generic-`IF` read agrees with them. Production wires the same
    /// engine-backed applier the replicas use.
    pub fn with_local_applier(
        mut self,
        applier: Arc<dyn crate::accord::apply::StorageApplier>,
    ) -> Self {
        self.local_applier = Some(applier);
        self
    }

    /// Supply a reader for the coordinator's own replica (generic-`IF` path).
    ///
    /// The coordinator's self-send is unreachable over the network, so without a
    /// local reader its own replica cannot contribute to the F+1 read agreement.
    /// Production wires the same engine-backed [`StorageReader`] the replicas use.
    ///
    /// [`StorageReader`]: crate::accord::apply::StorageReader
    pub fn with_local_reader(
        mut self,
        reader: Arc<dyn crate::accord::apply::StorageReader>,
    ) -> Self {
        self.local_reader = Some(reader);
        self
    }

    /// Supply the coordinator's OWN replica state machine so its local generic-`IF`
    /// read-vote performs the same dependency-wait the remote `AccordRead` handler
    /// does (block until every conflicting `t0 < t` has `Applied` locally).
    ///
    /// The coordinator's self-send is unreachable over the network, so without this
    /// the coordinator's local read-at-`t` would skip the dep-wait and could observe
    /// a key as absent while a genuinely concurrent contender (with a smaller `t`)
    /// is still mid-apply — the concurrent `INSERT IF NOT EXISTS` double-apply.
    /// Production wires the same [`AccordState`](crate::accord::handlers::AccordState)
    /// the node's inbound handlers use, backed by the same engine as the local
    /// reader.
    pub fn with_local_accord_state(mut self, state: crate::accord::handlers::AccordState) -> Self {
        self.local_accord_state = Some(state);
        self
    }

    /// The row bytes F+1 replicas agreed on during the generic-`IF` read-vote.
    ///
    /// `Some(serialized_mutation)` when the row existed at `t`; `None` when the
    /// row was absent at `t` or for the existence path. Valid only after a
    /// successful [`Self::run_transaction`].
    pub fn last_read_row(&self) -> Option<&[u8]> {
        self.last_read_row.as_deref()
    }

    /// Set the read-vote predicate for this transaction.
    ///
    /// Defaults to [`ReadPredicate::NotExists`](crate::accord::wire::ReadPredicate)
    /// (`INSERT IF NOT EXISTS`). For a generic `IF col=val`, the router supplies
    /// [`ReadPredicate::ReadRow`](crate::accord::wire::ReadPredicate) carrying the
    /// `keyspace`/`table` so replicas read the row at `t` and return its bytes.
    pub fn with_read_predicate(mut self, predicate: crate::accord::wire::ReadPredicate) -> Self {
        self.read_predicate = predicate;
        self
    }

    /// Supply the generic-`IF` condition gate.
    ///
    /// `gate(Some(row_bytes))` / `gate(None)` is called in the read-vote phase
    /// with the F+1-agreed row at `t` (or `None` if absent). It must return
    /// `true` iff the IF predicate holds (the write should apply). When it
    /// returns `false`, [`Self::run_transaction`] aborts with
    /// [`AccordDriverError::ConditionNotMet`] carrying the agreed row bytes —
    /// BEFORE the Apply phase — so a failing `IF col=val` never persists its
    /// mutation. This is the linearizable-LWT gate; see the field docs on
    /// `condition_gate`.
    ///
    /// The router wraps the canonical `ferrosa-cql` `eval_if_conditions` here so
    /// there is no forked evaluator.
    pub fn with_condition_gate(mut self, gate: ConditionGate) -> Self {
        self.condition_gate = Some(gate);
        self
    }

    /// Supply the per-key replica resolver for multi-shard fan-out (ADR-021).
    ///
    /// `resolve(key)` returns the replica host-ids that own `key`. With it set,
    /// the apply phase builds a multi-shard participant
    /// ([`ParticipantSet::from_per_key`](crate::accord::shard_quorum::ParticipantSet::from_per_key))
    /// and scopes each replica's `AccordApplyV2` to only the keys it owns.
    /// Production wires `WritePath::replicas_for_key`; `None` keeps the
    /// single-shard default. See the field docs on `per_key_replicas`.
    #[allow(clippy::type_complexity)]
    pub fn with_per_key_replicas(
        mut self,
        resolve: Arc<dyn Fn(&[u8]) -> Vec<uuid::Uuid> + Send + Sync>,
    ) -> Self {
        self.per_key_replicas = Some(resolve);
        self
    }

    /// Whether `replica` owns `key` under the current resolver.
    ///
    /// With no resolver this is the single-shard default — every replica owns
    /// every key (returns `true`). With a resolver, `replica` owns `key` iff it
    /// is in the key's resolved replica set.
    fn replica_owns_key(&self, replica: uuid::Uuid, key: &[u8]) -> bool {
        match &self.per_key_replicas {
            Some(resolve) => resolve(key).contains(&replica),
            None => true,
        }
    }

    /// Build the participant set for this transaction's write-set: per-key
    /// replica sets via the resolver (genuine multi-shard) when present, else the
    /// single-shard default (every key → the full replica list).
    fn participant_set(&self) -> crate::accord::shard_quorum::ParticipantSet {
        match &self.per_key_replicas {
            Some(resolve) => {
                let sets: Vec<Vec<uuid::Uuid>> =
                    self.write_set.iter().map(|e| resolve(&e.key)).collect();
                crate::accord::shard_quorum::ParticipantSet::from_per_key(&sets)
            }
            None => self.single_shard_participant(),
        }
    }

    /// Build the per-replica `AccordApplyV2` messages for the Apply fan-out: each
    /// replica's payload carries ONLY the write-set entries for keys it owns
    /// (the coordinator scopes; the replica trusts and applies what it received).
    /// Keyed by replica host-id, covering every id in `replica_ids`.
    fn apply_v2_messages(
        &self,
    ) -> Result<std::collections::HashMap<uuid::Uuid, Message>, AccordDriverError> {
        let txn_id = self.coordinator.txn_id;
        let mut out = std::collections::HashMap::with_capacity(self.replica_ids.len());
        for &peer in &self.replica_ids {
            let writes: Vec<crate::accord::wire::WriteSetEntry> = self
                .write_set
                .iter()
                .filter(|e| self.replica_owns_key(peer, &e.key))
                .cloned()
                .collect();
            let payload = crate::accord::wire::ApplyV2Payload { txn_id, writes };
            let bytes = bincode::serialize(&payload)
                .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
            out.insert(peer, Message::AccordApplyV2(Bytes::from(bytes)));
        }
        Ok(out)
    }

    /// Build the Apply-phase payload bytes for this transaction.
    ///
    /// The payload carries the encoded **mutation** as `result_data` — NOT the
    /// Accord partition key. Each replica decodes `result_data` as a commit-log
    /// `Mutation` and writes it to local storage; passing the key here would be
    /// a phantom write (storage applier would fail to decode, or worse, persist
    /// nothing). See `state_machine::handle_apply` for the consuming side.
    fn apply_payload_bytes(&self) -> Result<Vec<u8>, AccordDriverError> {
        use crate::accord::wire::ApplyPayload;
        let apply_payload = ApplyPayload {
            txn_id: self.coordinator.txn_id,
            result_data: self.mutation.clone(),
        };
        bincode::serialize(&apply_payload).map_err(|e| AccordDriverError::Codec(e.to_string()))
    }

    /// The multi-key Apply payload for this transaction: the full write-set the
    /// coordinator (and, once Phase 2 wires fan-out, each replica) applies. The
    /// coordinator's own-replica Apply iterates `writes`; the single-key path is
    /// the degenerate one-entry case. This is the canonical in-memory form that
    /// Phase 2 will serialize onto [`Message::AccordApplyV2`](ferrosa_net::Message).
    ///
    /// The production fan-out now builds per-replica payloads via
    /// [`Self::apply_v2_messages`]; this whole-write-set form is retained for
    /// tests asserting the degenerate single-key shape.
    #[cfg(test)]
    fn apply_v2_payload(&self) -> crate::accord::wire::ApplyV2Payload {
        crate::accord::wire::ApplyV2Payload {
            txn_id: self.coordinator.txn_id,
            writes: self.write_set.clone(),
        }
    }

    /// Finalize this transaction as a *no-write* commit across the cluster after
    /// the IF condition was found NOT to hold.
    ///
    /// A failed-IF LWT still committed (Accord ordered it), so on every replica
    /// it sits in `Committed` with a pending mutation it will never apply. Left
    /// there, it is a phantom dependency: a *later* transaction reading the same
    /// key at `t' > t` would dep-wait on it until timeout. We therefore broadcast
    /// an `Apply` carrying an EMPTY payload, which `handle_apply` treats as a
    /// no-write finalize — it advances the txn to `Applied`, wakes dep-waiters,
    /// and GCs the conflict index WITHOUT writing any row.
    ///
    /// Best-effort: this is a cleanup, not a correctness gate for THIS txn (which
    /// is already aborting). Failures are logged, not propagated.
    async fn finalize_no_write(&self) {
        use crate::accord::wire::ApplyPayload;
        let txn_id = self.coordinator.txn_id;

        // Finalize the coordinator's own replica state machine (its self-send is
        // unreachable). Empty payload => no-write finalize.
        if let Some(local_sm) = &self.local_accord_state {
            local_sm.lock().handle_apply(txn_id, Vec::new());
        }

        let payload = ApplyPayload {
            txn_id,
            result_data: Vec::new(),
        };
        let bytes = match bincode::serialize(&payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(txn_id = ?txn_id, error = %e, "accord: encode no-write finalize failed");
                return;
            }
        };
        let msg = Message::AccordApply(Bytes::from(bytes));

        let futs: Vec<_> = self
            .replica_ids
            .iter()
            .filter(|&&id| id != self.self_id)
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = msg.clone();
                async move { peers.send(peer_id, msg, Lane::Data).await }
            })
            .collect();
        for result in futures::future::join_all(futs).await {
            if let Err(e) = result {
                tracing::warn!(txn_id = ?txn_id, error = %e, "accord: no-write finalize RPC failed");
            }
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
            PreAcceptPayload, PreAcceptV2Payload, ReadVoteOkPayload, ReadVotePayload,
        };

        // Multi-key execution is wired end to end: PreAccept fans `AccordPreAcceptV2`
        // (all keys) so each replica unions dependencies across the whole write-set
        // (t_276e12), and the Apply phase fans a per-replica `AccordApplyV2` (scoped
        // to each replica's owned keys) under a per-shard participant, each replica
        // applying its whole write-set atomically. The representative `key` below is
        // used only for the single-key ReadVote (LWT IF-read) and v1 wire paths.
        let txn_id = self.coordinator.txn_id;
        let t0 = self.coordinator.t0;
        let key = self.coordinator.key.clone();
        let _rf = self.coordinator.rf; // available for future quorum checks

        // ------------------------------------------------------------------
        // Phase 1: PreAccept fanout
        // ------------------------------------------------------------------

        // Single-key keeps the v1 `AccordPreAccept` wire (byte-identical). Multi-key
        // sends `AccordPreAcceptV2` carrying every key, so each replica registers
        // the txn under all of them and returns the UNION of dependencies across
        // keys — serializing transactions that overlap on a non-first key (t_276e12).
        let pa_msg = if self.write_set.len() == 1 {
            let pa_payload = PreAcceptPayload {
                txn_id,
                t0,
                key: key.clone(),
                ballot: BallotNumber(0),
                epoch: 0,
            };
            let pa_bytes = bincode::serialize(&pa_payload)
                .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
            Message::AccordPreAccept(Bytes::from(pa_bytes))
        } else {
            let keys: Vec<Vec<u8>> = self.write_set.iter().map(|w| w.key.clone()).collect();
            let pa_payload = PreAcceptV2Payload {
                txn_id,
                t0,
                keys,
                ballot: BallotNumber(0),
                epoch: 0,
            };
            let pa_bytes = bincode::serialize(&pa_payload)
                .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
            Message::AccordPreAcceptV2(Bytes::from(pa_bytes))
        };

        let mut decision = CoordinatorDecision::Pending;

        // The coordinator is itself a replica for these keys in the common case
        // (the node serving the request is a replica). It must process its OWN
        // PreAccept LOCALLY — registering the txn under every key and computing
        // real deps, exactly as a remote replica's handler does — and MUST NOT
        // send PreAccept to itself: a node is never in its own peer map, so the
        // self-send fails "unknown peer" and (at RF=1) loses the only vote,
        // stalling the fast-path at `Pending` → "Accord quorum unavailable". This
        // mirrors the self-handling the Commit/Apply phases already do
        // (`record_node_ack(self_id)` + filtering self from the fan-out).
        let self_is_replica =
            self.self_id != uuid::Uuid::nil() && self.replica_ids.contains(&self.self_id);
        let has_remote_replica = self.replica_ids.iter().any(|&p| p != self.self_id);
        // Process our OWN PreAccept locally when we are a replica — a node is never
        // in its own peer map, so a self-send fails "unknown peer" and loses the
        // coordinator's vote. The coordinator is a replica in the common case (the
        // node serving the request), so this is the norm, not an edge case.
        //
        // The self-vote is required for LIVENESS whenever `fast_quorum_size(rf)`
        // equals the full replica count (RF≥3): the coordinator gathers only the
        // RF-1 REMOTE votes, so if they all AGREE the driver can neither fast-commit
        // (needs all RF, i.e. the coordinator's own vote too) nor slow-path (no
        // disagreement) — it stalls at `Pending` → "Accord quorum unavailable". The
        // self-vote supplies the missing RF-th vote.
        //
        // The one case we must NOT self-vote is when it would SHORT-CIRCUIT the
        // remote fan-out: at RF=2, `fast_quorum_size(2) == 1`, so a lone local vote
        // fast-paths and skips telling the remote replica — which breaks the LWT
        // ReadVote dep-wait serialization (the remote must register the txn to
        // serialize concurrent INSERT IF NOT EXISTS). RF=1 has no remote to skip, so
        // the sole-replica self-vote is always correct. Hence: self-vote when we are
        // the sole replica OR when `fast_quorum_size(rf) > 1` (RF≥3), where the
        // self-vote can never reach quorum alone and the fan-out always runs.
        let self_vote_cannot_short_circuit =
            !has_remote_replica || fast_quorum_size(self.coordinator.rf) > 1;
        if self_is_replica && !self.coordinator.is_leaseholder && self_vote_cannot_short_circuit {
            if let Some(local_sm) = &self.local_accord_state {
                let keys: Vec<&[u8]> = self.write_set.iter().map(|w| w.key.as_slice()).collect();
                let resp =
                    local_sm
                        .lock()
                        .handle_preaccept_multi(txn_id, t0, &keys, BallotNumber(0), 0);
                if let crate::accord::state_machine::SmResponse::PreAcceptOK { t, deps, .. } = resp
                {
                    decision = self.coordinator.handle_preaccept_ok(PreAcceptResponse {
                        from: self.coordinator.node_id,
                        t,
                        deps,
                    });
                } else {
                    // The local replica did not vote. Dropping this silently
                    // costs the round a vote it was counting on and makes the
                    // failure indistinguishable from a remote timeout: an
                    // RF=3 non-leaseholder then holds only its two peer votes,
                    // short of the 3-vote fast quorum. Say so.
                    tracing::warn!(
                        txn_id = ?txn_id,
                        response = ?resp,
                        "accord: local PreAccept self-vote was not PreAcceptOK; \
                         this round proceeds without the coordinator's own vote"
                    );
                }
            }
        }

        // Fanout to the REMOTE replicas only (self is handled locally above / by
        // the leaseholder implicit vote); a self-send would hit "unknown peer".
        if decision == CoordinatorDecision::Pending {
            let futs: Vec<_> = self
                .replica_ids
                .iter()
                .filter(|&&peer_id| peer_id != self.self_id)
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

            // Count the outcomes so a failed round says WHY, not just that it
            // failed. An empty AccordPreAcceptOK is the replica's encoding for
            // a Nack, a persist failure, and an unexpected state alike, so
            // without this a quorum failure is indistinguishable from silence.
            let mut votes = 0usize;
            let mut no_votes = 0usize;
            let mut rpc_failures = 0usize;

            for result in &responses {
                match result {
                    Ok((_peer_id, Message::AccordPreAcceptOK(b))) if !b.is_empty() => {
                        let ok: PreAcceptOkPayload = bincode::deserialize(b)
                            .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
                        votes += 1;
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
                    Ok((peer_id, _)) => {
                        // The peer answered without a usable vote. The wire
                        // shape cannot say whether that was a Nack, a persist
                        // failure, or an unexpected state — so record it and
                        // let the round summary carry the count.
                        no_votes += 1;
                        tracing::debug!(
                            txn_id = ?txn_id,
                            peer = ?peer_id,
                            "accord: PreAccept returned no vote (empty or unexpected response)"
                        );
                    }
                    Err(e) => {
                        rpc_failures += 1;
                        tracing::warn!(
                            txn_id = ?txn_id,
                            error = %e,
                            "accord: PreAccept RPC failed (non-fatal, continuing)"
                        );
                    }
                }
            }

            // Every response that will ever arrive has arrived, so a fast
            // quorum is now provably unreachable. If a slow quorum voted, the
            // round commits through the Accept phase instead of stalling.
            if decision == CoordinatorDecision::Pending {
                decision = self.coordinator.finalize_preaccept();
                if let CoordinatorDecision::NeedAccept { .. } = decision {
                    tracing::debug!(
                        txn_id = ?txn_id,
                        votes,
                        "accord: fan-out exhausted short of a fast quorum; \
                         falling back to the Accept phase"
                    );
                }
            }

            if decision == CoordinatorDecision::Pending {
                tracing::warn!(
                    txn_id = ?txn_id,
                    peers = responses.len(),
                    votes,
                    no_votes,
                    rpc_failures,
                    slow_quorum = slow_quorum_size(self.coordinator.rf),
                    "accord: PreAccept round reached no decision; a replica that \
                     answers without voting reports the same empty payload whether \
                     it rejected the txn or failed to persist it"
                );
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
                let accept_deps: Vec<TxnId> = deps.iter().copied().collect();

                let mut ac_decision = CoordinatorDecision::Pending;

                // The coordinator processes its OWN Accept LOCALLY and fans out to
                // the REMOTE replicas only. A node is never in its own peer map, so
                // an Accept self-send fails "unknown peer" and loses the
                // coordinator's own vote — the slow path then needs EVERY remote
                // replica, so under concurrency (which is what pushes a txn onto
                // the slow path in the first place) many transactions fail "Accord
                // quorum unavailable". This mirrors the PreAccept self-vote.
                let self_is_replica =
                    self.self_id != uuid::Uuid::nil() && self.replica_ids.contains(&self.self_id);
                if self_is_replica {
                    if let Some(local_sm) = &self.local_accord_state {
                        let resp = local_sm.lock().handle_accept(
                            txn_id,
                            t0,
                            t,
                            accept_deps.clone(),
                            BallotNumber(1),
                        );
                        if let crate::accord::state_machine::SmResponse::AcceptOK { .. } = resp {
                            ac_decision = self.coordinator.handle_accept_ok(AcceptResponse {
                                from: self.coordinator.node_id,
                                ballot: BallotNumber(1),
                                deps: accept_deps.clone(),
                            });
                        }
                    }
                }

                if ac_decision == CoordinatorDecision::Pending {
                    let ac_futs: Vec<_> = self
                        .replica_ids
                        .iter()
                        .filter(|&&peer_id| peer_id != self.self_id)
                        .map(|&peer_id| {
                            let peers = Arc::clone(&self.peers);
                            let msg = ac_msg.clone();
                            async move { peers.send(peer_id, msg, Lane::Data).await }
                        })
                        .collect();

                    let ac_responses = futures::future::join_all(ac_futs).await;

                    for result in &ac_responses {
                        match result {
                            Ok(Message::AccordAcceptOK(b)) if !b.is_empty() => {
                                let ok: AcceptOkPayload = bincode::deserialize(b)
                                    .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
                                let resp = AcceptResponse {
                                    from: ok.txn_id.0.node, // node ID embedded in txn_id
                                    ballot: BallotNumber(1),
                                    deps: accept_deps.clone(),
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

        // The coordinator drove the protocol, so it is an implicit ack for both
        // Commit and Apply; `self_is_replica` is also consumed by the local-apply
        // below.
        let self_is_replica = self.replica_ids.contains(&self_id) && self_id != uuid::Uuid::nil();

        // Per-shard quorum: every shard the write-set touches must independently
        // reach its slow quorum. A single global counter would let one shard
        // commit while another is a minority — the cross-shard non-atomicity
        // Accord exists to prevent. `participant_set` resolves per-key replica
        // sets when a multi-shard resolver is wired, else collapses to one shard
        // (the behavior-preserving single-key / single-replica-set default).
        let participant = self.participant_set();
        if !self
            .quorum_broadcast(commit_msg, &participant, |r| r.is_ok())
            .await
        {
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

        // The read-vote phase is the LWT IF-condition gate. An unconditional
        // transaction (`ReadPredicate::Always`) has no IF, so skip the whole
        // phase and apply directly — otherwise the existence/row read-vote would
        // wrongly gate an UPDATE to an existing row.
        if !matches!(
            self.read_predicate,
            crate::accord::wire::ReadPredicate::Always
        ) {
            let read_payload = ReadVotePayload {
                txn_id,
                t: commit_t,
                key: key.clone(),
                predicate: self.read_predicate.clone(),
            };
            let read_bytes = bincode::serialize(&read_payload)
                .map_err(|e| AccordDriverError::Codec(e.to_string()))?;
            let read_msg = Message::AccordRead(Bytes::from(read_bytes));

            let mut votes_false = 0usize;
            let mut dissenting_row: Vec<u8> = Vec::new();
            // For the generic ReadRow predicate: collect each replica's row-at-`t`
            // bytes so we can require F+1 *agreement* on the row state before the
            // coordinator evaluates the IF predicate. Disagreement is a correctness
            // failure (non-linearizable read) and must abort, never silently pick one.
            let mut read_rows: Vec<Vec<u8>> = Vec::new();
            let is_generic = matches!(
                self.read_predicate,
                crate::accord::wire::ReadPredicate::ReadRow { .. }
            );

            // The coordinator's own replica is not reachable over the network
            // (self-send fails). For the generic path it must contribute its local
            // read-at-`t` so that, with RF=2 (sq=2), F+1 agreement is achievable and
            // the result is deterministic across all replicas. The applier already
            // persisted earlier conflicting txns locally before this read (dep-wait).
            if is_generic {
                if let crate::accord::wire::ReadPredicate::ReadRow { keyspace, table } =
                    &self.read_predicate
                {
                    // Prefer the local state machine when wired: it performs the SAME
                    // dep-wait the remote handler does (block until every conflicting
                    // `t0 < t` has Applied locally) before reading at `t`. This is what
                    // serializes a genuinely concurrent contender's write ahead of this
                    // read — without it the coordinator's own replica could read the key
                    // as absent while a smaller-`t` contender is mid-apply (the
                    // concurrent INSERT IF NOT EXISTS double-apply). On dep-wait timeout
                    // we ABSTAIN (push no row) so F+1 agreement fails loud rather than
                    // reading stale.
                    if let Some(local_sm) = &self.local_accord_state {
                        if crate::accord::handlers::await_conflicting_deps_applied(
                            local_sm, &key, commit_t,
                        )
                        .await
                        {
                            let row = local_sm
                                .lock()
                                .read_row_bytes_at(keyspace, table, &key, commit_t);
                            read_rows.push(row.unwrap_or_default());
                        } else {
                            tracing::error!(
                                txn_id = ?txn_id,
                                "accord: coordinator local read-vote dep-wait timed out — abstaining"
                            );
                            // Abstain: contribute no local read. F+1 agreement then
                            // fails loud below rather than treating a stale read as truth.
                        }
                    } else if let Some(reader) = &self.local_reader {
                        match reader.read_row_at(keyspace, table, &key, commit_t) {
                            Ok(bytes) => read_rows.push(bytes.unwrap_or_default()),
                            Err(e) => {
                                return Err(AccordDriverError::Network(format!(
                                    "coordinator local read-at-t failed: {e}"
                                )));
                            }
                        }
                    }
                }
            }

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
                            Ok(vote) => {
                                if is_generic {
                                    // Generic IF: replica returns the row bytes at `t`
                                    // (condition_holds is a neutral true). Collect for
                                    // F+1 agreement; the coordinator evaluates the
                                    // predicate authoritatively below.
                                    read_rows.push(vote.current_row.clone());
                                } else if !vote.condition_holds {
                                    // INSERT IF NOT EXISTS existence path.
                                    votes_false += 1;
                                    if dissenting_row.is_empty() {
                                        dissenting_row = vote.current_row.clone();
                                    }
                                }
                            }
                            Err(_) => {
                                // pre-Gap-4 replica or parse error: treat as
                                // condition_holds=true (forward-compatible default).
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

            if is_generic {
                // Require F+1 replicas to agree on the SAME row bytes at `t`. This is
                // the linearizable read: a divergent read is non-linearizable and must
                // abort (fail loud) rather than have the coordinator guess.
                let agreed = agreed_row(&read_rows, sq);
                let agreed_row_bytes = match agreed {
                    Some(row) => {
                        self.last_read_row = if row.is_empty() {
                            None
                        } else {
                            Some(row.clone())
                        };
                        row
                    }
                    None => {
                        return Err(AccordDriverError::Network(format!(
                            "generic IF read-vote lacked F+1 ({sq}) agreement on the row at t \
                         (got {} reads) — refusing a non-linearizable LWT",
                            read_rows.len()
                        )));
                    }
                };

                // GATE THE WRITE on the IF condition. The coordinator owns the table
                // schema (via the injected gate, which wraps the canonical
                // eval_if_conditions); it evaluates the predicate against the
                // F+1-agreed, linearizable row-at-`t` and ABORTS before the Apply
                // phase when the condition does not hold. Without this the generic
                // path would persist its mutation unconditionally and still report
                // [applied]=false — a lost-update / wrong-[applied] data-loss bug.
                //
                // Determinism: every replica sees the same `t` and the same agreed
                // row bytes, so the gate's verdict is identical everywhere.
                if let Some(gate) = &self.condition_gate {
                    let row_arg: Option<&[u8]> = if agreed_row_bytes.is_empty() {
                        None
                    } else {
                        Some(agreed_row_bytes.as_slice())
                    };
                    if !gate(row_arg) {
                        tracing::info!(
                            txn_id = ?txn_id,
                            "accord: generic IF condition not met — [applied]=false, no Apply"
                        );
                        // Finalize this committed-but-not-applied txn as a no-write
                        // across replicas so it does not linger as a phantom dep that
                        // would stall later reads' dep-wait on this key.
                        self.finalize_no_write().await;
                        return Err(AccordDriverError::ConditionNotMet {
                            current_row: agreed_row_bytes,
                        });
                    }
                }
            } else {
                // F+1 matching votes decide the outcome.
                // Only return ConditionNotMet if F+1 replicas explicitly voted false.
                if votes_false >= sq {
                    tracing::info!(
                        txn_id = ?txn_id,
                        votes_false,
                        sq,
                        "accord: IF condition not met — [applied]=false"
                    );
                    // Finalize this committed-but-not-applied txn as a no-write
                    // across replicas so it does not linger as a phantom dep that
                    // would stall later reads' dep-wait on this key.
                    self.finalize_no_write().await;
                    return Err(AccordDriverError::ConditionNotMet {
                        current_row: dissenting_row,
                    });
                }
            }
        } // end read-vote phase (skipped for ReadPredicate::Always)

        // ------------------------------------------------------------------
        // Phase 5: Apply broadcast (Gap 5 — dep-wait + storage write)
        //
        // Broadcast Apply to all remote replicas with the mutation payload.
        // Count the coordinator itself as an implicit apply (it drove the
        // protocol and already processed the commit). Wait for remote ApplyOK
        // to reach F+1 total before returning the LWT result.
        // ------------------------------------------------------------------

        // Coordinator's OWN replica Commit + Apply. A node is never in its own
        // peer map, so its self-addressed Commit/Apply RPCs are unreachable — it
        // must drive its OWN state machine through the same Commit → Apply path a
        // remote replica takes on receiving `AccordCommit` + `AccordApply`. Doing
        // so (a) persists the mutation via the dep-ordered apply engine and, just
        // as importantly, (b) advances the SM to `Applied` and fires the
        // applied-notify. Without (b) a later linearizable read served by THIS node
        // dep-waits forever on a conflict that is durably written but never marked
        // Applied in the SM (the read-visibility bug: committed writes invisible to
        // a subsequent SERIAL read on the coordinator).
        //
        // When the SM is present (production cluster) it is the single apply path —
        // its apply engine is idempotent on `(txn_id, key, t)`, so there is no
        // double-apply. The bare `local_applier` fallback is for tests / no-SM
        // setups that persist but have no state machine to advance.
        if self_is_replica {
            let owned_writes: Vec<Vec<u8>> = self
                .write_set
                .iter()
                .filter(|e| !e.mutation.is_empty())
                .filter(|e| self.replica_owns_key(self_id, &e.key))
                .map(|e| e.mutation.clone())
                .collect();
            if !owned_writes.is_empty() {
                if let Some(local_sm) = &self.local_accord_state {
                    let mut sm = local_sm.lock();
                    // Commit then apply on this node's own state machine, mirroring
                    // the `handle_commit` + `handle_apply_writeset` RPC handlers.
                    sm.handle_commit(txn_id, t0, commit_t, commit_deps.iter().copied().collect());
                    sm.handle_apply_writeset(txn_id, owned_writes);
                } else if let Some(applier) = &self.local_applier {
                    let deps: Vec<TxnId> = commit_deps.iter().copied().collect();
                    let owned: Vec<crate::accord::apply::ApplyMutation> = self
                        .write_set
                        .iter()
                        .filter(|e| !e.mutation.is_empty())
                        .filter(|e| self.replica_owns_key(self_id, &e.key))
                        .map(|e| crate::accord::apply::ApplyMutation {
                            data: e.mutation.clone(),
                            t: commit_t,
                            deps: deps.clone(),
                        })
                        .collect();
                    applier.apply_writeset(txn_id, owned).map_err(|e| {
                        AccordDriverError::Network(format!("coordinator local apply failed: {e}"))
                    })?;
                }
            }
        }

        // Apply quorum: the SAME per-shard rule as Commit (reusing the
        // `participant` built above). An Apply ack is an `AccordApplyOK` for this
        // txn — an empty body, or a payload whose `txn_id` matches.
        let apply_txn = txn_id;
        let is_apply_ok = move |r: &ferrosa_net::error::Result<Message>| {
            matches!(r, Ok(Message::AccordApplyOK(b))
                if b.is_empty()
                    || bincode::deserialize::<ApplyOkPayload>(b)
                        .map(|ok| ok.txn_id == apply_txn)
                        .unwrap_or(false))
        };

        // Single-key keeps the v1 `AccordApply` wire (byte-identical). Multi-key
        // sends each replica a per-replica `AccordApplyV2` scoped to the keys it
        // owns, so a replica never persists a key it is not a replica for.
        let apply_ok = if self.write_set.len() == 1 {
            let apply_bytes = self.apply_payload_bytes()?;
            let apply_msg = Message::AccordApply(Bytes::from(apply_bytes));
            self.quorum_broadcast(apply_msg, &participant, is_apply_ok)
                .await
        } else {
            let per_peer = self.apply_v2_messages()?;
            self.quorum_broadcast_per_peer(
                &participant,
                |peer_id| {
                    per_peer
                        .get(&peer_id)
                        .cloned()
                        .expect("every replica_id has a per-peer AccordApplyV2 message")
                },
                is_apply_ok,
            )
            .await
        };
        if !apply_ok {
            tracing::error!(
                txn_id = ?txn_id,
                unmet = ?participant.shards.len(),
                "accord: Apply quorum not reached — LWT result may not be durable"
            );
            return Err(AccordDriverError::ApplyQuorumUnavailable);
        }

        tracing::info!(
            txn_id = ?txn_id,
            "accord: Apply phase complete — [applied]=true"
        );

        Ok((commit_t, commit_deps))
    }

    /// Build the single-shard participant set for the current write-set: every
    /// key maps to the full `replica_ids`, i.e. one shard. This is the
    /// behavior-preserving default (per-shard quorum over one shard ==
    /// `slow_quorum_size(rf)`); the per-key `ring.replicas(token, rf)` fan-out
    /// that produces multiple shards is a follow-up increment.
    fn single_shard_participant(&self) -> crate::accord::shard_quorum::ParticipantSet {
        let keys: Vec<Vec<u8>> = self.write_set.iter().map(|e| e.key.clone()).collect();
        let replica_ids = self.replica_ids.clone();
        crate::accord::shard_quorum::ParticipantSet::build(&keys, |_| replica_ids.clone())
    }

    /// Fan `msg` out to the replica set and decide success by **per-shard** slow
    /// quorum: every shard in `participant` must independently reach
    /// `slow_quorum_size(shard_rf)`. The coordinator's own replica is an implicit
    /// ack (it drove the protocol and its self-send is unreachable). `is_ack`
    /// decides whether a peer's response counts. Returns true iff every shard
    /// reached quorum.
    pub(crate) async fn quorum_broadcast(
        &self,
        msg: Message,
        participant: &crate::accord::shard_quorum::ParticipantSet,
        is_ack: impl Fn(&ferrosa_net::error::Result<Message>) -> bool,
    ) -> bool {
        // Same message to every peer — the degenerate case of the per-peer
        // fan-out (used by PreAccept/Commit/Read and single-key Apply, whose
        // payload is key-independent or the same for all replicas).
        self.quorum_broadcast_per_peer(participant, |_| msg.clone(), is_ack)
            .await
    }

    /// Like [`quorum_broadcast`](Self::quorum_broadcast) but builds a **distinct
    /// message per peer** via `build_msg(peer_id)`. This is what lets the
    /// multi-key Apply send each replica an `AccordApplyV2` scoped to only the
    /// keys it owns, while still deciding success by the same per-shard quorum.
    pub(crate) async fn quorum_broadcast_per_peer(
        &self,
        participant: &crate::accord::shard_quorum::ParticipantSet,
        build_msg: impl Fn(uuid::Uuid) -> Message,
        is_ack: impl Fn(&ferrosa_net::error::Result<Message>) -> bool,
    ) -> bool {
        let self_id = self.self_id;
        let mut quorum = participant.quorum();
        if self.replica_ids.contains(&self_id) && self_id != uuid::Uuid::nil() {
            quorum.record_node_ack(self_id);
        }

        let futs: Vec<_> = self
            .replica_ids
            .iter()
            .filter(|&&id| id != self_id)
            .map(|&peer_id| {
                let peers = Arc::clone(&self.peers);
                let msg = build_msg(peer_id);
                async move { (peer_id, peers.send(peer_id, msg, Lane::Data).await) }
            })
            .collect();

        for (peer_id, result) in futures::future::join_all(futs).await {
            if is_ack(&result) {
                quorum.record_node_ack(peer_id);
            } else if let Err(e) = &result {
                tracing::warn!(error = %e, peer = %peer_id, "accord: quorum broadcast RPC failed");
            }
        }
        quorum.all_reached()
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

    /// A replica that answers without voting must be counted, not skipped.
    ///
    /// The replica encodes a `Nack`, a state machine that could not persist,
    /// and any unexpected response all as an EMPTY `AccordPreAcceptOK`. The
    /// coordinator skipped every one with a bare `Ok(_) => {}`, so a round that
    /// collected no votes reported "quorum unavailable" and nothing more.
    ///
    /// That is how the 2026-06 FileSyncWriter failure hid: PreAccept could not
    /// persist, so no replica voted, and the only symptom was an unexplained
    /// quorum error. Counting the outcomes does not make an empty payload
    /// informative by itself, but it distinguishes "nobody answered" from
    /// "everybody answered and nobody voted" -- which are different bugs.
    #[test]
    fn a_reply_without_a_vote_is_counted_separately_from_a_failed_rpc() {
        let voted = Message::AccordPreAcceptOK(bytes::Bytes::from_static(b"payload"));
        let empty = Message::AccordPreAcceptOK(bytes::Bytes::new());

        assert_eq!(
            classify_preaccept_response(Ok(&voted)),
            PreAcceptOutcome::Vote
        );
        assert_eq!(
            classify_preaccept_response(Ok(&empty)),
            PreAcceptOutcome::NoVote,
            "an empty payload is an answer, not a missing one"
        );
        assert_eq!(
            classify_preaccept_response(Err(())),
            PreAcceptOutcome::RpcFailed,
            "a peer that never answered is a different failure from one that \
             answered without voting"
        );
    }

    /// An unexpected message type is a non-vote, not a vote.
    ///
    /// Worth pinning: the vote arm matches a NON-EMPTY AccordPreAcceptOK
    /// specifically, so a differently-shaped reply must not be mistaken for
    /// agreement.
    #[test]
    fn an_unexpected_message_is_not_a_vote() {
        let other = Message::AccordAcceptOK(bytes::Bytes::from_static(b"not-preaccept"));
        assert_eq!(
            classify_preaccept_response(Ok(&other)),
            PreAcceptOutcome::NoVote
        );
    }

    /// F+1 agreement means a MAJORITY agrees, not that every replica does.
    ///
    /// `agreed_row` documented "Require F+1 replicas to agree on the SAME row
    /// bytes at `t`", and the error it feeds says "lacked F+1 (2) agreement".
    /// The implementation asked a different question:
    ///
    /// ```text
    /// let first = &reads[0];
    /// if reads.iter().all(|r| r == first) { ... }
    /// ```
    ///
    /// That is unanimity. On RF=3 a single lagging or slow replica outvoted a
    /// genuine 2-of-3 majority, so every generic-IF LWT was refused as
    /// non-linearizable when it was in fact perfectly decidable.
    ///
    /// Observed live on 2026-08-22 against the three-node cluster:
    ///
    /// ```text
    /// generic IF read-vote lacked F+1 (2) agreement on the row at t
    /// (got 3 reads) — refusing a non-linearizable LWT
    /// ```
    ///
    /// Three reads returned, F+1 was 2, and it still refused.
    #[test]
    fn a_majority_agreeing_is_enough_even_when_one_replica_differs() {
        let a = b"row-at-t".to_vec();
        let stale = b"stale-row".to_vec();

        let agreed = agreed_row(&[a.clone(), a.clone(), stale], 2);

        assert_eq!(
            agreed,
            Some(a),
            "two of three replicas agreed and F+1 is two; a third disagreeing \
             read must not veto a decidable quorum"
        );
    }

    /// Unanimity still agrees — the common case must not regress.
    #[test]
    fn unanimous_reads_agree() {
        let a = b"row".to_vec();
        assert_eq!(agreed_row(&[a.clone(), a.clone(), a.clone()], 2), Some(a));
    }

    /// No value reaching F+1 is genuinely undecidable, and must stay refused.
    /// This is the case the unanimity check was conflating with the one above.
    #[test]
    fn no_value_reaching_the_quorum_is_refused() {
        assert_eq!(
            agreed_row(&[b"x".to_vec(), b"y".to_vec(), b"z".to_vec()], 2),
            None,
            "three different reads means no value has two votes"
        );
    }

    /// A split with no majority is refused even though a value repeats.
    #[test]
    fn a_tie_below_the_quorum_is_refused() {
        assert_eq!(
            agreed_row(
                &[b"x".to_vec(), b"x".to_vec(), b"y".to_vec(), b"y".to_vec()],
                3
            ),
            None,
            "2 and 2 with a quorum of 3 leaves the row undecided"
        );
    }

    /// Too few reads to decide at all.
    #[test]
    fn fewer_reads_than_the_quorum_is_refused() {
        let a = b"row".to_vec();
        assert_eq!(agreed_row(std::slice::from_ref(&a), 2), None);
        assert_eq!(agreed_row(&[], 1), None);
    }

    /// An empty row (the key does not exist) is a legitimate agreed VALUE, not
    /// an absence of agreement. `IF v = ...` against a missing row must be able
    /// to decide "condition not met" rather than fail the transaction.
    #[test]
    fn agreement_on_an_empty_row_is_still_agreement() {
        assert_eq!(
            agreed_row(&[Vec::new(), Vec::new(), b"other".to_vec()], 2),
            Some(Vec::new()),
            "two replicas agreeing the row is absent is a decided read"
        );
    }
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
    fn finalize_preaccept_takes_slow_path_when_fanout_exhausts_with_slow_quorum() {
        // RF=3, non-leaseholder. Only two replicas ever answer (the third is
        // down, or the local self-vote was lost). Both AGREE with t0, so
        // `handle_preaccept_ok` correctly stays Pending — a third agreeing
        // vote would still unlock the 1-RTT fast path.
        //
        // But once the fan-out is exhausted no further response can arrive.
        // A slow quorum (2 of 3) is present and sufficient to commit via the
        // Accept phase, so the round MUST fall back to the slow path. Before
        // this existed the coordinator sat at Pending forever and the caller
        // reported "Accord quorum unavailable" on a healthy cluster.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);
        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        for from in [1, 2] {
            assert_eq!(
                coord.handle_preaccept_ok(PreAcceptResponse {
                    from,
                    t: t0,
                    deps: vec![],
                }),
                CoordinatorDecision::Pending,
                "must keep waiting for a possible fast quorum"
            );
        }

        assert_eq!(
            coord.finalize_preaccept(),
            CoordinatorDecision::NeedAccept {
                t: t0,
                deps: HashSet::new(),
            }
        );
        assert_eq!(coord.phase, CoordinatorPhase::Accepting);
        assert_eq!(coord.rtt_count(), 1);
    }

    #[test]
    fn finalize_preaccept_stays_pending_below_slow_quorum() {
        // One vote out of RF=3 is not enough to commit by any path. Finalizing
        // must NOT invent a decision — the caller has to report the round as
        // genuinely quorum-unavailable.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);
        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        coord.handle_preaccept_ok(PreAcceptResponse {
            from: 1,
            t: t0,
            deps: vec![],
        });

        assert_eq!(coord.finalize_preaccept(), CoordinatorDecision::Pending);
        assert_eq!(coord.phase, CoordinatorPhase::PreAccepting);
    }

    #[test]
    fn disagreement_at_slow_quorum_decides_without_waiting_for_finalize() {
        // A response that proposes a different timestamp (or carries deps)
        // makes the fast path unreachable immediately, so the EXISTING slow-
        // quorum branch decides on the spot and `finalize_preaccept` never
        // sees the round. This pins that split: `finalize_preaccept` is only
        // ever reached in the all-agree case, which is why it can safely
        // forward the merged state.
        let t0 = make_ts(1000);
        let later = make_ts(5000);
        let dep = make_txn_id(9, 400);
        let txn_id = make_txn_id(1, 1000);
        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        coord.handle_preaccept_ok(PreAcceptResponse {
            from: 1,
            t: t0,
            deps: vec![],
        });
        let decided = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 2,
            t: later,
            deps: vec![dep],
        });

        let mut expected = HashSet::new();
        expected.insert(dep);
        assert_eq!(
            decided,
            CoordinatorDecision::NeedAccept {
                t: later,
                deps: expected,
            },
            "a disagreeing vote at slow quorum must decide immediately"
        );

        // Already decided -> finalizing is a no-op and must not re-enter the
        // phase transition or double-count the RTT.
        assert_eq!(coord.finalize_preaccept(), CoordinatorDecision::Pending);
        assert_eq!(coord.phase, CoordinatorPhase::Accepting);
        assert_eq!(coord.rtt_count(), 1);
    }

    #[test]
    fn finalize_preaccept_is_a_noop_once_a_decision_was_reached() {
        // RF=1: the single response decides immediately. Finalizing after a
        // decision must not re-run the phase transition or double-count an RTT.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);
        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 1, false);

        let decided = coord.handle_preaccept_ok(PreAcceptResponse {
            from: 1,
            t: t0,
            deps: vec![],
        });
        assert!(matches!(
            decided,
            CoordinatorDecision::FastPathCommit { .. }
        ));

        assert_eq!(coord.finalize_preaccept(), CoordinatorDecision::Pending);
        assert_eq!(coord.phase, CoordinatorPhase::FastPathCommit);
        assert_eq!(coord.rtt_count(), 1);
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

    // -----------------------------------------------------------------------
    // Apply payload carries the real MUTATION, not the partition key.
    //
    // Increment 3 of the LWT data path: the coordinator's Apply phase must hand
    // each replica the encoded commit-log `Mutation` (`result_data`), which the
    // storage applier decodes and writes. Carrying the raw partition key here is
    // the phantom-write bug — the applier cannot decode a bare key as a
    // `Mutation`, so nothing durable is written even though `[applied]=true` is
    // returned. This test pins `result_data == mutation` (and `!= key`).
    // -----------------------------------------------------------------------
    #[test]
    fn apply_payload_carries_mutation_not_key() {
        use crate::accord::wire::ApplyPayload;
        use ferrosa_common::accord::HybridLogicalClock;
        use ferrosa_net::config::NetConfig;
        use ferrosa_net::peer::{PeerEventListener, PeerManager};
        use std::sync::Arc;

        struct NoopListener;
        impl PeerEventListener for NoopListener {
            fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
            fn on_peer_recovered(&self, _: uuid::Uuid) {}
            fn on_peer_failed(&self, _: uuid::Uuid) {}
        }

        let self_uuid = uuid::Uuid::new_v4();
        let node_id =
            u64::from_be_bytes(self_uuid.as_bytes()[..8].try_into().expect("uuid 16 bytes"));
        let peers = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            self_uuid,
            Arc::new(NoopListener),
        ));
        let clock = HybridLogicalClock::new(node_id, 0);

        // A partition key distinct from the encoded mutation bytes.
        let key = b"pk-bytes".to_vec();
        let mutation = b"ENCODED-MUTATION-BYTES".to_vec();

        let driver = AccordCoordinatorDriver::new(
            node_id,
            vec![self_uuid],
            peers,
            true,
            &clock,
            key.clone(),
            mutation.clone(),
        );

        let bytes = driver
            .apply_payload_bytes()
            .expect("apply payload must serialize");
        let decoded: ApplyPayload =
            bincode::deserialize(&bytes).expect("apply payload must round-trip");

        assert_eq!(
            decoded.result_data, mutation,
            "Apply result_data MUST be the encoded mutation (not the partition key) — \
             a replica decodes result_data as a commit-log Mutation and writes it"
        );
        assert_ne!(
            decoded.result_data, key,
            "Apply result_data must NOT be the raw partition key — that is the \
             phantom-write bug this increment closes"
        );
    }

    // -----------------------------------------------------------------------
    // Phase 1 (multi-key Accord): the additive `new_multi` API + V2 wire.
    // -----------------------------------------------------------------------

    struct NoopListener;
    impl ferrosa_net::peer::PeerEventListener for NoopListener {
        fn on_peer_connected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_disconnected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_suspected(&self, _: ferrosa_net::rpc::handler::PeerId) {}
        fn on_peer_recovered(&self, _: uuid::Uuid) {}
        fn on_peer_failed(&self, _: uuid::Uuid) {}
    }

    /// Build (node_id, self_uuid, peers) for a single-node driver test.
    fn single_node_peers() -> (
        u64,
        uuid::Uuid,
        std::sync::Arc<ferrosa_net::peer::PeerManager>,
    ) {
        use ferrosa_net::config::NetConfig;
        use ferrosa_net::peer::PeerManager;
        use std::sync::Arc;
        let self_uuid = uuid::Uuid::new_v4();
        let node_id =
            u64::from_be_bytes(self_uuid.as_bytes()[..8].try_into().expect("uuid 16 bytes"));
        let peers = Arc::new(PeerManager::new(
            Arc::new(NetConfig::default()),
            self_uuid,
            Arc::new(NoopListener),
        ));
        (node_id, self_uuid, peers)
    }

    /// A single-key transaction is the degenerate one-entry write-set: `new`
    /// delegates to `new_multi`, the V2 Apply payload has exactly one write, and
    /// the v1 Apply wire bytes still carry that single mutation unchanged.
    #[test]
    fn new_multi_single_entry_is_degenerate_single_key() {
        use crate::accord::wire::ApplyPayload;
        use ferrosa_common::accord::HybridLogicalClock;

        let (node_id, self_uuid, peers) = single_node_peers();
        let clock = HybridLogicalClock::new(node_id, 0);
        let key = b"pk-bytes".to_vec();
        let mutation = b"ENCODED-MUTATION".to_vec();

        let via_new = AccordCoordinatorDriver::new(
            node_id,
            vec![self_uuid],
            peers.clone(),
            true,
            &clock,
            key.clone(),
            mutation.clone(),
        );
        let via_multi = AccordCoordinatorDriver::new_multi(
            node_id,
            vec![self_uuid],
            peers,
            true,
            &clock,
            vec![(key.clone(), mutation.clone())],
        );

        for driver in [&via_new, &via_multi] {
            let v2 = driver.apply_v2_payload();
            assert_eq!(
                v2.writes.len(),
                1,
                "single-key txn has a one-entry write-set"
            );
            assert_eq!(v2.writes[0].key, key);
            assert_eq!(v2.writes[0].mutation, mutation);

            // v1 Apply wire bytes are byte-identical in shape: result_data == mutation.
            let bytes = driver
                .apply_payload_bytes()
                .expect("apply payload serializes");
            let decoded: ApplyPayload =
                bincode::deserialize(&bytes).expect("apply payload round-trips");
            assert_eq!(decoded.result_data, mutation);
        }
    }

    /// A genuine multi-key transaction is now WIRED for execution (the
    /// `MultiKeyNotYetExecutable` guard is gone): its driver builds a per-replica
    /// Apply fan-out covering EVERY key, and (on a single-node RF=1 cluster) the
    /// coordinator owns and would apply both. The full multi-node commit→apply
    /// round-trip is the CI-gated cross-shard e2e; here we assert the wiring is
    /// in place and no key is dropped from the fan-out.
    #[test]
    fn multi_key_driver_fans_out_every_key_no_guard() {
        use ferrosa_common::accord::HybridLogicalClock;

        let (node_id, self_uuid, peers) = single_node_peers();
        let clock = HybridLogicalClock::new(node_id, 0);

        let driver = AccordCoordinatorDriver::new_multi(
            node_id,
            vec![self_uuid],
            peers,
            true,
            &clock,
            vec![
                (b"key-1".to_vec(), b"mutation-1".to_vec()),
                (b"key-2".to_vec(), b"mutation-2".to_vec()),
            ],
        );

        // Single shard (no resolver) → the coordinator's own replica owns BOTH
        // keys, so its Apply payload carries the full write-set — nothing dropped.
        let msgs = driver.apply_v2_messages().expect("per-peer apply messages");
        let mine = decode_v2(msgs.get(&self_uuid).expect("coordinator has a payload"));
        assert_eq!(
            mine.writes.len(),
            2,
            "the multi-key write-set fans out every key (no MultiKeyNotYetExecutable guard)"
        );
        let keys: Vec<&[u8]> = mine.writes.iter().map(|w| w.key.as_slice()).collect();
        assert!(keys.contains(&b"key-1".as_slice()) && keys.contains(&b"key-2".as_slice()));
    }

    // -----------------------------------------------------------------------
    // Phase 2: per-shard quorum, exercised through the real `quorum_broadcast`
    // + the transport seam with a mock that returns controllable per-node acks.
    // -----------------------------------------------------------------------

    use crate::accord::shard_quorum::ParticipantSet;
    use crate::accord::transport::AccordTransport;

    /// A mock transport: each peer either acks (canned `Ok` response) or fails
    /// (`Err`), per a configured map. Routes nothing — it only decides ack/fail.
    struct MockTransport {
        behavior: std::collections::HashMap<uuid::Uuid, bool>,
        ok: Message,
    }

    #[async_trait::async_trait]
    impl AccordTransport for MockTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            _msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            if *self.behavior.get(&host_id).unwrap_or(&false) {
                Ok(self.ok.clone())
            } else {
                Err(ferrosa_net::error::NetError::Timeout(
                    "mock node down".into(),
                ))
            }
        }
    }

    /// Driver whose coordinator is NOT one of the replicas (node_id 999 matches
    /// no `from_u128(small)` replica), so the quorum is decided purely by the
    /// mock's per-node responses — no implicit self-ack to muddy the assertion.
    fn driver_with(
        transport: Arc<dyn AccordTransport>,
        replica_ids: Vec<uuid::Uuid>,
    ) -> AccordCoordinatorDriver {
        let clock = HybridLogicalClock::new(999, 0);
        AccordCoordinatorDriver::new_multi_with_transport(
            999,
            replica_ids,
            transport,
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
    }

    /// Regression for the deployed-Accord failure: the coordinator is itself the
    /// SOLE replica (RF=1) — the normal production case where the node serving the
    /// request is a replica for the key. It must process its OWN PreAccept locally
    /// (via its Accord state machine) and MUST NOT send PreAccept to itself: a node
    /// is never in its own peer map, so a self-send fails "unknown peer" and, at
    /// RF=1, loses the only vote — which is why every deployed transaction failed
    /// "Accord quorum unavailable".
    #[tokio::test]
    async fn preaccept_self_is_processed_locally_never_sent_rf1() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let host = uuid::Uuid::from_u128(0xC0DE);
        let node_id = u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap());

        // The coordinator's own Accord state machine (its local replica).
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(node_id, Arc::new(MockSyncWriter::new())),
        ));

        // A transport that records every send and never contains self — exactly
        // like the production PeerManager, which has no entry for the node's own id.
        let transport = Arc::new(CapturingTransport {
            sent: parking_lot::Mutex::new(std::collections::HashMap::new()),
        });
        let clock = HybridLogicalClock::new(node_id, 0);

        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            node_id,
            vec![host], // RF=1: self is the ONLY replica
            transport.clone(),
            false, // not leaseholder — mirrors the production committer's flag
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()));

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "RF=1 sole-replica transaction must commit via a local self-vote; got {result:?}"
        );
        assert!(
            !transport.sent.lock().contains_key(&host),
            "coordinator must never send PreAccept to itself (its id is not in the peer map)"
        );
    }

    /// A transport that drives a coordinator-is-a-replica transaction onto the
    /// slow (Accept) path and then makes ONE remote replica time out during
    /// Accept. It records every peer that received an `AccordAccept`.
    ///
    /// - PreAccept: `remote2` proposes a higher timestamp + a dependency, so the
    ///   two remote votes disagree → the coordinator needs an Accept round.
    /// - Accept: `remote1` votes; `remote2` times out. Slow quorum (RF=3) is 2,
    ///   so the transaction can ONLY commit if the coordinator counts its OWN
    ///   Accept vote locally. Under the pre-fix code the coordinator instead sent
    ///   `AccordAccept` to itself (which fails "unknown peer" in production and
    ///   is silently dropped here), so it saw one remote vote and failed.
    /// - Commit/Apply: every remote acks, so only the Accept phase is stressed.
    struct SlowPathSelfVoteTransport {
        remote1: uuid::Uuid,
        remote2: uuid::Uuid,
        t0: Timestamp,
        conflict_t: Timestamp,
        conflict_dep: TxnId,
        accept_targets: parking_lot::Mutex<Vec<uuid::Uuid>>,
    }

    fn node_id_of(host: uuid::Uuid) -> u64 {
        u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap())
    }

    #[async_trait::async_trait]
    impl AccordTransport for SlowPathSelfVoteTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{AcceptOkPayload, AcceptPayload, PreAcceptOkPayload};
            match msg {
                Message::AccordPreAccept(_) | Message::AccordPreAcceptV2(_) => {
                    let (t, deps) = if host_id == self.remote1 {
                        (self.t0, vec![])
                    } else if host_id == self.remote2 {
                        (self.conflict_t, vec![self.conflict_dep])
                    } else {
                        panic!("PreAccept to an unexpected host {host_id} (self must be local)");
                    };
                    let payload = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t,
                        deps,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                Message::AccordAccept(b) => {
                    self.accept_targets.lock().push(host_id);
                    if host_id == self.remote1 {
                        let ap: AcceptPayload = bincode::deserialize(&b).unwrap();
                        let ok = AcceptOkPayload { txn_id: ap.txn_id };
                        Ok(Message::AccordAcceptOK(Bytes::from(
                            bincode::serialize(&ok).unwrap(),
                        )))
                    } else if host_id == self.remote2 {
                        // remote2 is slow during Accept — its vote never arrives,
                        // so the coordinator's own local vote is what reaches quorum.
                        Err(ferrosa_net::error::NetError::Timeout(
                            "slow remote during Accept".into(),
                        ))
                    } else {
                        // A self-send would land here under the pre-fix code — the
                        // recorded target is what the test's assertion catches.
                        Err(ferrosa_net::error::NetError::Timeout(
                            "unexpected Accept target (self must be local)".into(),
                        ))
                    }
                }
                // Commit / Apply / anything else: ack so only Accept is stressed.
                _ => Ok(Message::AccordApplyOK(Bytes::new())),
            }
        }
    }

    /// Regression for the Accept-phase self-send bug (sibling of the PreAccept
    /// fix). When the coordinator is itself a replica (RF=3), the slow path must
    /// process its OWN Accept LOCALLY and fan `AccordAccept` out to the REMOTE
    /// replicas only. A node is never in its own peer map, so an Accept self-send
    /// fails "unknown peer" and loses the coordinator's vote — and because a
    /// transaction only reaches the slow path under contention (exactly when a
    /// remote is likely slow), losing that vote made ~61% of concurrent
    /// transactions fail "Accord quorum unavailable" in the live Elle run.
    #[tokio::test]
    async fn accept_self_is_processed_locally_never_sent_rf3() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128(0xC0DE);
        let remote1 = uuid::Uuid::from_u128(0x1111);
        let remote2 = uuid::Uuid::from_u128(0x2222);
        let self_node = node_id_of(self_host);

        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));

        let t0 = make_ts(1000);
        let transport = Arc::new(SlowPathSelfVoteTransport {
            remote1,
            remote2,
            t0,
            conflict_t: make_ts(2000),
            conflict_dep: make_txn_id(7, 500),
            accept_targets: parking_lot::Mutex::new(Vec::new()),
        });
        let clock = HybridLogicalClock::new(self_node, 0);

        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2], // RF=3, coordinator is a replica
            transport.clone(),
            false, // not leaseholder
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()));

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "slow-path transaction must commit via the coordinator's LOCAL Accept \
             vote when one remote is slow (self + remote1 = slow quorum 2); got {result:?}"
        );

        let targets = transport.accept_targets.lock();
        assert!(
            !targets.contains(&self_host),
            "coordinator must never send AccordAccept to itself (its id is not in \
             the peer map); sent to {targets:?}"
        );
        assert!(
            targets.contains(&remote1) && targets.contains(&remote2),
            "coordinator must fan Accept out to BOTH remote replicas; sent to {targets:?}"
        );
    }

    /// A transport whose remote replicas AGREE with the coordinator: each echoes
    /// back the coordinator's proposed `t0` with no dependencies, so the fast path
    /// hinges entirely on whether the coordinator counts its OWN vote. Records
    /// every peer that received a PreAccept.
    struct AgreeingPreAcceptTransport {
        preaccept_targets: parking_lot::Mutex<Vec<uuid::Uuid>>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for AgreeingPreAcceptTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{PreAcceptOkPayload, PreAcceptPayload};
            match msg {
                Message::AccordPreAccept(b) => {
                    self.preaccept_targets.lock().push(host_id);
                    // Echo the coordinator's own t0 so this replica genuinely agrees.
                    let pa: PreAcceptPayload = bincode::deserialize(&b).unwrap();
                    let payload = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: pa.t0,
                        deps: vec![],
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                // Commit / Apply / anything else: ack.
                _ => Ok(Message::AccordApplyOK(Bytes::new())),
            }
        }
    }

    /// Regression for the RF≥3 stall: when the coordinator is a replica and every
    /// REMOTE replica agrees on the PreAccept, the driver reaches `fast_quorum` (=RF)
    /// only if the coordinator counts its OWN PreAccept vote. Without it the RF-1
    /// remote votes can neither fast-commit (need all RF) nor slow-path (no
    /// disagreement), so the driver stalls at `Pending` → QuorumUnavailable. This is
    /// the exact live failure: `decision=Pending pa_votes=2 rf=3` on every commit.
    #[tokio::test]
    async fn preaccept_self_vote_reaches_fast_quorum_rf3() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128(0xC0DE);
        let remote1 = uuid::Uuid::from_u128(0x1111);
        let remote2 = uuid::Uuid::from_u128(0x2222);
        let self_node = node_id_of(self_host);

        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));

        let transport = Arc::new(AgreeingPreAcceptTransport {
            preaccept_targets: parking_lot::Mutex::new(Vec::new()),
        });
        let clock = HybridLogicalClock::new(self_node, 0);

        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2], // RF=3, coordinator is a replica
            transport.clone(),
            false, // not leaseholder
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()));

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "RF=3 coordinator-is-replica txn with all remotes AGREEING must fast-commit \
             via the coordinator's OWN PreAccept vote (fast_quorum=3), not stall at \
             Pending; got {result:?}"
        );

        let targets = transport.preaccept_targets.lock();
        assert!(
            !targets.contains(&self_host),
            "coordinator must never send PreAccept to itself; sent to {targets:?}"
        );
    }

    /// Two RF=3 shards: A = n[0..3], B = n[3..6].
    fn two_shards(n: &[uuid::Uuid]) -> ParticipantSet {
        ParticipantSet::build(&[b"ka".to_vec(), b"kb".to_vec()], |k| {
            if k == b"ka" {
                vec![n[0], n[1], n[2]]
            } else {
                vec![n[3], n[4], n[5]]
            }
        })
    }

    fn six_nodes() -> Vec<uuid::Uuid> {
        (1u128..=6).map(uuid::Uuid::from_u128).collect()
    }

    /// A capturing transport: records the exact `Message` sent to each peer, and
    /// acks every send. Lets a test assert the per-replica `AccordApplyV2`
    /// fan-out scoped each replica to only the keys it owns.
    struct CapturingTransport {
        sent: parking_lot::Mutex<std::collections::HashMap<uuid::Uuid, Message>>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for CapturingTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            self.sent.lock().insert(host_id, msg);
            Ok(Message::AccordApplyOK(Bytes::new()))
        }
    }

    type KeyResolver = Arc<dyn Fn(&[u8]) -> Vec<uuid::Uuid> + Send + Sync>;

    fn per_key_resolver(n: Vec<uuid::Uuid>) -> KeyResolver {
        Arc::new(move |key: &[u8]| {
            if key == b"ka" {
                vec![n[0], n[1], n[2]]
            } else {
                vec![n[3], n[4], n[5]]
            }
        })
    }

    fn decode_v2(m: &Message) -> crate::accord::wire::ApplyV2Payload {
        match m {
            Message::AccordApplyV2(b) => bincode::deserialize(b).expect("v2 decodes"),
            other => panic!("expected AccordApplyV2, got {other:?}"),
        }
    }

    #[test]
    fn apply_v2_messages_scope_each_replica_to_its_owned_keys() {
        let n = six_nodes();
        let clock = HybridLogicalClock::new(999, 0);
        let driver = AccordCoordinatorDriver::new_multi_with_transport(
            999,
            n.clone(),
            Arc::new(CapturingTransport {
                sent: parking_lot::Mutex::new(std::collections::HashMap::new()),
            }),
            false,
            &clock,
            vec![
                (b"ka".to_vec(), b"mut-a".to_vec()),
                (b"kb".to_vec(), b"mut-b".to_vec()),
            ],
        )
        .with_per_key_replicas(per_key_resolver(n.clone()));

        let msgs = driver.apply_v2_messages().expect("build per-peer messages");

        // Shard-A replicas get only ka; shard-B replicas get only kb.
        for id in &n[0..3] {
            let p = decode_v2(msgs.get(id).expect("shard-A replica has a message"));
            assert_eq!(p.writes.len(), 1, "shard-A replica gets only its owned key");
            assert_eq!(p.writes[0].key, b"ka");
            assert_eq!(p.writes[0].mutation, b"mut-a");
        }
        for id in &n[3..6] {
            let p = decode_v2(msgs.get(id).expect("shard-B replica has a message"));
            assert_eq!(p.writes.len(), 1, "shard-B replica gets only its owned key");
            assert_eq!(p.writes[0].key, b"kb");
            assert_eq!(p.writes[0].mutation, b"mut-b");
        }
    }

    #[test]
    fn apply_v2_messages_without_resolver_send_full_writeset_to_every_replica() {
        // No resolver → single shard → every replica owns every key.
        let n = six_nodes();
        let clock = HybridLogicalClock::new(999, 0);
        let driver = AccordCoordinatorDriver::new_multi_with_transport(
            999,
            n.clone(),
            Arc::new(CapturingTransport {
                sent: parking_lot::Mutex::new(std::collections::HashMap::new()),
            }),
            false,
            &clock,
            vec![
                (b"ka".to_vec(), b"mut-a".to_vec()),
                (b"kb".to_vec(), b"mut-b".to_vec()),
            ],
        );

        let msgs = driver.apply_v2_messages().expect("build per-peer messages");
        for id in &n {
            assert_eq!(
                decode_v2(msgs.get(id).unwrap()).writes.len(),
                2,
                "single-shard: every replica receives the full write-set"
            );
        }
    }

    #[tokio::test]
    async fn quorum_broadcast_per_peer_delivers_scoped_message_to_each_replica() {
        let n = six_nodes();
        let clock = HybridLogicalClock::new(999, 0);
        let transport = Arc::new(CapturingTransport {
            sent: parking_lot::Mutex::new(std::collections::HashMap::new()),
        });
        let driver = AccordCoordinatorDriver::new_multi_with_transport(
            999,
            n.clone(),
            transport.clone(),
            false,
            &clock,
            vec![
                (b"ka".to_vec(), b"mut-a".to_vec()),
                (b"kb".to_vec(), b"mut-b".to_vec()),
            ],
        )
        .with_per_key_replicas(per_key_resolver(n.clone()));

        let per_peer = driver.apply_v2_messages().unwrap();
        let reached = driver
            .quorum_broadcast_per_peer(
                &driver.participant_set(),
                |peer| per_peer.get(&peer).cloned().unwrap(),
                |r| r.is_ok(),
            )
            .await;
        assert!(reached, "every shard acked → quorum reached");

        // Each replica actually received ITS scoped payload over the wire.
        let sent = transport.sent.lock();
        assert_eq!(decode_v2(sent.get(&n[0]).unwrap()).writes[0].key, b"ka");
        assert_eq!(decode_v2(sent.get(&n[4]).unwrap()).writes[0].key, b"kb");
    }

    #[tokio::test]
    async fn quorum_broadcast_blocks_when_one_shard_is_a_minority() {
        let n = six_nodes();
        // Shard A all ack; shard B: only n[3] acks (1/3 < quorum 2).
        let behavior = [
            (n[0], true),
            (n[1], true),
            (n[2], true),
            (n[3], true),
            (n[4], false),
            (n[5], false),
        ]
        .into_iter()
        .collect();
        let mock = Arc::new(MockTransport {
            behavior,
            ok: Message::AccordApplyOK(Bytes::new()),
        });
        let driver = driver_with(mock, n.clone());

        let reached = driver
            .quorum_broadcast(Message::AccordCommit(Bytes::new()), &two_shards(&n), |r| {
                r.is_ok()
            })
            .await;
        assert!(
            !reached,
            "shard B is a minority (1/3) — a global counter (4/6 acks) would wrongly pass"
        );
    }

    #[tokio::test]
    async fn quorum_broadcast_succeeds_when_every_shard_has_quorum() {
        let n = six_nodes();
        // Each shard at 2/3 → quorum in both.
        let behavior = [
            (n[0], true),
            (n[1], true),
            (n[2], false),
            (n[3], true),
            (n[4], true),
            (n[5], false),
        ]
        .into_iter()
        .collect();
        let mock = Arc::new(MockTransport {
            behavior,
            ok: Message::AccordApplyOK(Bytes::new()),
        });
        let driver = driver_with(mock, n.clone());

        let reached = driver
            .quorum_broadcast(Message::AccordCommit(Bytes::new()), &two_shards(&n), |r| {
                r.is_ok()
            })
            .await;
        assert!(reached, "both shards at 2/3 → quorum in each");
    }
}
