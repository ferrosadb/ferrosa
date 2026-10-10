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
use std::sync::OnceLock;

use crate::accord::transport::AccordTransport;
use bytes::Bytes;
use ferrosa_common::accord::{BallotNumber, HybridLogicalClock, Timestamp, TxnId, TxnPhase};
use ferrosa_net::codec::Lane;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;

const PREACCEPT_FAST_PATH_TIMEOUT_ENV: &str = "FERROSA_ACCORD_PREACCEPT_FAST_PATH_TIMEOUT_MS";
const DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS: u64 = 1_000;

fn parse_preaccept_fast_path_timeout(value: Option<&str>) -> Result<std::time::Duration, String> {
    let Some(value) = value else {
        return Ok(std::time::Duration::from_millis(
            DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS,
        ));
    };
    let millis = value
        .parse::<u64>()
        .map_err(|error| format!("expected positive milliseconds: {error}"))?;
    if millis == 0 {
        return Err("value must be greater than zero".into());
    }
    let timeout = std::time::Duration::from_millis(millis);
    if tokio::time::Instant::now().checked_add(timeout).is_none() {
        return Err("value exceeds the supported timer range".into());
    }
    Ok(timeout)
}

fn configured_preaccept_fast_path_timeout() -> std::time::Duration {
    static CONFIG: OnceLock<std::time::Duration> = OnceLock::new();
    *CONFIG.get_or_init(|| {
        let value = match std::env::var(PREACCEPT_FAST_PATH_TIMEOUT_ENV) {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(value)) => {
                tracing::error!(
                    variable = PREACCEPT_FAST_PATH_TIMEOUT_ENV,
                    value = %value.to_string_lossy(),
                    default_ms = DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS,
                    "invalid non-UTF-8 Accord PreAccept fast-path timeout; using the default"
                );
                return std::time::Duration::from_millis(DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS);
            }
        };
        match parse_preaccept_fast_path_timeout(value.as_deref()) {
            Ok(timeout) => timeout,
            Err(error) => {
                tracing::error!(
                    variable = PREACCEPT_FAST_PATH_TIMEOUT_ENV,
                    value = value.as_deref().unwrap_or_default(),
                    %error,
                    default_ms = DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS,
                    "invalid Accord PreAccept fast-path timeout; using the default"
                );
                std::time::Duration::from_millis(DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS)
            }
        }
    })
}

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
    /// Replica IDs whose PreAcceptOK has already been counted.
    preaccept_responders: HashSet<u64>,
    /// Collected AcceptOK responses.
    accept_responses: Vec<AcceptResponse>,
    /// Replica IDs whose AcceptOK has already been counted.
    accept_responders: HashSet<u64>,

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
            preaccept_responders: HashSet::new(),
            accept_responses: Vec::new(),
            accept_responders: HashSet::new(),
            merged_t: t0,
            merged_deps: HashSet::new(),
            rtt_count: 0,
        };

        // Leaseholder optimization: the coordinator itself implicitly votes
        // for t0 with empty deps (it owns the range, no conflicts seen locally).
        if is_leaseholder {
            coord.preaccept_responders.insert(node_id);
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

        // A retry can arrive after this phase has already completed. Treat it
        // as an idempotent duplicate before enforcing the phase transition.
        if self.preaccept_responders.contains(&response.from) {
            return CoordinatorDecision::Pending;
        }

        // The round has already left PreAccepting. That happens when a slow
        // quorum answered and `finalize_preaccept` advanced to Accepting while a
        // third replica's PreAcceptOK was still in flight — a replica can answer
        // late under load or retries. Such a vote may no longer influence this
        // round's decision, so it is IGNORED rather than panicked on. Panicking
        // here killed the writer task, which surfaced as `Accord apply quorum
        // unavailable` and a lost PostgreSQL connection on a healthy cluster.
        if self.phase != CoordinatorPhase::PreAccepting {
            tracing::debug!(
                from = response.from,
                phase = ?self.phase,
                "accord: ignoring a late PreAcceptOK after the round left PreAccepting"
            );
            return CoordinatorDecision::Pending;
        }

        // Networks and transports may retry a response. A replica can only
        // contribute one vote to this phase, and retries must not influence
        // the timestamp or dependency union.
        self.preaccept_responders.insert(response.from);

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

        // Ignore a retry even when the original reply already completed the
        // slow path and moved the coordinator out of Accepting.
        if self.accept_responders.contains(&response.from) {
            return CoordinatorDecision::Pending;
        }

        assert_eq!(
            self.phase,
            CoordinatorPhase::Accepting,
            "handle_accept_ok called in wrong phase: {:?}",
            self.phase
        );

        // Count quorum members, not packets: duplicate AcceptOK deliveries are
        // common under retries but cannot stand in for another replica.
        self.accept_responders.insert(response.from);

        self.accept_responses.push(response.clone());

        // Accept may discover conflicts that were absent from the PreAccept
        // responses (for example, a delayed concurrent PreAccept). Preserve
        // those dependencies through the slow-path decision.
        self.merged_deps.extend(response.deps);

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
    /// The transaction's write-set has more keys than a node's Accord
    /// conflict-index capacity can hold. A PreAccept registers the transaction
    /// under EVERY key and is all-or-nothing, so a write-set larger than the
    /// index is refused by *every* replica and is indistinguishable from a
    /// cluster fault unless named here. Raised before the protocol registers
    /// anything, so no finalize is owed.
    WriteSetExceedsCapacity {
        /// Number of keys in this transaction's write-set.
        keys: usize,
        /// The node's configured conflict-index capacity (see
        /// [`crate::accord::state_machine::configured_conflict_index_capacity`]).
        capacity: usize,
    },
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
    /// A PostgreSQL transaction's Accord replicas have observed a newer
    /// conflicting marker timestamp than its captured MVCC snapshot.
    SnapshotStale,
    /// The transaction exceeded the operator-configured dependency-wait bound
    /// and was **abandoned**: it was never applied (rolled back) and was finalized
    /// so it no longer blocks later transactions on its keys. The transaction is
    /// NOT committed — the client is told so explicitly and may safely retry.
    ///
    /// This subsumes the former `ApplyQuorumUnavailable`: when the apply quorum
    /// could not be reached within the bound, the transaction is now abandoned
    /// rather than left as a permanently blocking `Committed` entry.
    TxnAbandoned {
        /// The bound that expired.
        timeout: std::time::Duration,
    },
}

impl std::fmt::Display for AccordDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QuorumUnavailable => write!(f, "Accord quorum unavailable"),
            Self::WriteSetExceedsCapacity { keys, capacity } => write!(
                f,
                "transaction write-set of {keys} keys exceeds the Accord conflict-index capacity of \
                 {capacity}; raise FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY (or lower \
                 FERROSA_POSTGRES_MAX_TXN_WRITES) so the consensus layer can register every \
                 write-set the SQL layer accepts"
            ),
            Self::Network(e) => write!(f, "Accord network error: {e}"),
            Self::Codec(e) => write!(f, "Accord codec error: {e}"),
            Self::ConditionNotMet { .. } => write!(f, "Accord LWT condition not met"),
            Self::SnapshotStale => write!(f, "PostgreSQL MVCC snapshot is stale"),
            // The `abandoned:` prefix is load-bearing. The committer erases this
            // typed error to a `reason: String` before it reaches the PostgreSQL
            // front end, which classifies it by prefix — the same convention
            // `is_backpressure()` uses for `overloaded:`. It is what turns an
            // abandoned transaction into a RETRYABLE 40001 (serialization
            // failure) instead of an opaque 58000 fault, so keep it stable.
            Self::TxnAbandoned { timeout } => write!(
                f,
                "abandoned: transaction exceeded the {}s dependency-wait bound and was \
                 rolled back (NOT committed); safe to retry",
                timeout.as_secs_f64()
            ),
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
    /// Maximum wait for a possible final fast-path PreAccept vote.
    preaccept_fast_path_timeout: std::time::Duration,
    /// The node's Accord conflict-index capacity (see
    /// [`crate::accord::state_machine::configured_conflict_index_capacity`]).
    ///
    /// A PreAccept registers the transaction under every key it writes and is
    /// all-or-nothing, so a write-set larger than this can never reach a quorum —
    /// every replica refuses it. Bounding the driver here turns that opaque
    /// "Accord quorum unavailable" into a named, actionable error before the
    /// protocol runs. Overridable via
    /// [`Self::with_conflict_index_capacity`] so the guard is testable.
    conflict_index_capacity: usize,
    /// Network seam: `PeerManager` in production, a mock in tests.
    peers: Arc<dyn AccordTransport>,
    /// IDs of the replicas for this transaction's token range.
    replica_ids: Vec<uuid::Uuid>,
    /// Optional PostgreSQL MVCC timestamp checked by replicas during V2
    /// PreAccept. CQL callers leave this unset.
    snapshot_ts: Option<Timestamp>,
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
    //
    // Votes are compared in canonical form (t_b986c335): the same row can be
    // encoded differently by replicas on different builds — an old one sends
    // its stored nanosecond stamps, an upgraded one the same stamps normalised
    // to microseconds (t_cf637b6e) — and byte equality would fail F+1 on every
    // LWT touching a legacy row until the roll completes.
    let canonical: Vec<Vec<u8>> = reads.iter().map(|r| canonical_read_vote(r)).collect();
    let mut counts: std::collections::HashMap<&[u8], usize> = std::collections::HashMap::new();
    for read in &canonical {
        let n = counts.entry(read.as_slice()).or_insert(0);
        *n += 1;
        if *n >= quorum {
            return Some(read.clone());
        }
    }
    None
}

/// The canonical encoding of one read vote: decoded as a
/// [`ferrosa_storage::Mutation`] (which normalises legacy nanosecond
/// timestamps) and re-encoded. An empty vote (row absent) stays empty. Bytes
/// that do not decode are compared as they are: the coordinator decodes the
/// agreed row again before evaluating the condition and fails loud there, so
/// a corrupt vote is never silently treated as agreement.
fn canonical_read_vote(read: &[u8]) -> Vec<u8> {
    if read.is_empty() {
        return Vec::new();
    }
    match ferrosa_storage::Mutation::deserialize_from(read) {
        Ok(m) => {
            let mut buf = vec![0u8; m.serialized_size()];
            m.serialize_into(&mut buf);
            buf
        }
        Err(e) => {
            tracing::debug!(%e, bytes = read.len(), "read vote does not decode; compared raw");
            read.to_vec()
        }
    }
}

async fn collect_until_decided<Fut, Response>(
    responses: impl IntoIterator<Item = Fut>,
    mut is_decided: impl FnMut(&Response) -> bool,
) where
    Fut: std::future::Future<Output = Response>,
{
    use futures::StreamExt;

    let mut pending = futures::stream::FuturesUnordered::from_iter(responses);
    while let Some(response) = pending.next().await {
        if is_decided(&response) {
            break;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistenceVoteDecision {
    Apply,
    ConditionNotMet,
    QuorumUnavailable,
}

/// Decide an `INSERT IF NOT EXISTS` read-vote from explicit replica votes.
///
fn decide_existence_votes(
    votes_true: usize,
    votes_false: usize,
    quorum: usize,
) -> ExistenceVoteDecision {
    if votes_false >= quorum {
        ExistenceVoteDecision::ConditionNotMet
    } else if votes_true >= quorum {
        ExistenceVoteDecision::Apply
    } else {
        ExistenceVoteDecision::QuorumUnavailable
    }
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
    #[doc(hidden)]
    pub fn new_multi_with_transport(
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
            preaccept_fast_path_timeout: configured_preaccept_fast_path_timeout(),
            conflict_index_capacity:
                crate::accord::state_machine::configured_conflict_index_capacity(),
            peers,
            replica_ids,
            snapshot_ts: None,
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

    /// Override the wait for a possible fast-path PreAccept response.
    ///
    /// Production defaults to `FERROSA_ACCORD_PREACCEPT_FAST_PATH_TIMEOUT_MS`
    /// (1000 ms). Expiry never counts as a vote: the driver enters Accept only
    /// after the coordinator has already collected a valid slow quorum.
    pub fn with_preaccept_fast_path_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.preaccept_fast_path_timeout = timeout;
        self
    }

    /// Override the node's Accord conflict-index capacity for this driver.
    ///
    /// Production takes it from
    /// [`configured_conflict_index_capacity`](crate::accord::state_machine::configured_conflict_index_capacity);
    /// this exists so the oversized-write-set guard is testable without touching
    /// the process environment (which `set_var` makes racy under parallel tests).
    #[must_use]
    pub fn with_conflict_index_capacity(mut self, capacity: usize) -> Self {
        self.conflict_index_capacity = capacity;
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

    /// Attach a PostgreSQL snapshot timestamp for replica-side stale-snapshot
    /// validation. This is specific to PostgreSQL; CQL transactions leave it unset.
    pub fn with_postgres_snapshot(mut self, snapshot_ts: Timestamp) -> Self {
        self.snapshot_ts = Some(snapshot_ts);
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
    /// Not a correctness gate for THIS txn (which is already aborting), so the
    /// caller does not wait for the remote half: the local replica is finalized
    /// before returning, and the remote fan-out runs detached, retrying each
    /// replica until it acks (see [`deliver_no_write_finalize`]).
    async fn finalize_no_write(&self) {
        self.finalize_no_write_locally().await;
        tokio::spawn(self.no_write_fanout());
    }

    /// Finalize the coordinator's own replica state machine as a no-write (its
    /// self-send is unreachable). Empty payload => no-write finalize.
    ///
    /// Routed through [`crate::accord::handlers::on_state_machine`] like every
    /// other state-machine access from async code: `handle_apply` fsyncs the
    /// protocol log while holding the state machine's mutex, which must not
    /// happen on an async worker.
    async fn finalize_no_write_locally(&self) {
        if let Some(local_sm) = &self.local_accord_state {
            let txn_id = self.coordinator.txn_id;
            crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                sm.handle_apply(txn_id, Vec::new())
            })
            .await;
        }
    }

    /// The remote half of [`Self::finalize_no_write`]: an empty-payload `Apply`
    /// to every remote replica. Owns everything it needs, so a caller that must
    /// not wait on it (see [`Self::run_transaction`]) can spawn it.
    fn no_write_fanout(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        use crate::accord::wire::ApplyPayload;
        let txn_id = self.coordinator.txn_id;
        let peers = Arc::clone(&self.peers);
        let remotes: Vec<uuid::Uuid> = self
            .replica_ids
            .iter()
            .copied()
            .filter(|&id| id != self.self_id)
            .collect();

        async move {
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
            let undelivered =
                deliver_no_write_finalize(&peers, remotes, &msg, txn_id, NO_WRITE_FINALIZE_RETRY)
                    .await;
            if !undelivered.is_empty() {
                tracing::error!(
                    txn_id = ?txn_id,
                    replicas = ?undelivered,
                    attempts = NO_WRITE_FINALIZE_RETRY.attempts,
                    "accord: no-write finalize never reached these replicas; each keeps the \
                     transaction as a pending conflict, so reads and snapshot barriers on its \
                     keys there will dep-wait and abstain until it is recovered"
                );
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
    ///
    /// # Failing before Apply
    ///
    /// A transaction that fails in phases 1-4 is finalized as a no-write on
    /// every replica: locally before the error is returned, and on the remote
    /// replicas by a spawned fan-out. By then it may be
    /// `PreAccepted`, `Accepted` or `Committed` on any replica, and no other
    /// node will ever finish it, so left alone it would sit in the conflict
    /// index for good: every later read-vote on its keys would dep-wait on it,
    /// time out and abstain. Nothing has been written yet (the mutation travels
    /// only in the Apply phase), so a no-write finalize is the true outcome.
    pub async fn run_transaction(
        &mut self,
    ) -> Result<(Timestamp, HashSet<TxnId>), AccordDriverError> {
        // A write-set larger than a node's conflict index can never be decided:
        // the PreAccept registration is all-or-nothing, so every replica refuses
        // it and the caller sees an opaque "Accord quorum unavailable". Name it
        // here, before the protocol registers anything (so no finalize is owed),
        // rather than let a 1.1M-row transactional COPY fail as a mystery. See
        // `state_machine::resolve_conflict_index_capacity` for the coherence rule
        // (conflict-index capacity must cover every write-set the front end
        // admits) that this enforces on the coordinator side.
        let capacity = self.conflict_index_capacity;
        if self.write_set.len() > capacity {
            tracing::error!(
                txn_id = ?self.coordinator.txn_id,
                keys = self.write_set.len(),
                capacity,
                "accord: refusing a transaction whose write-set exceeds the node's conflict-index \
                 capacity; no replica could register it (see FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY)"
            );
            return Err(AccordDriverError::WriteSetExceedsCapacity {
                keys: self.write_set.len(),
                capacity,
            });
        }
        let (commit_t, commit_deps) = match self.order_and_gate().await {
            Ok(ordered) => ordered,
            Err(e) => {
                // ConditionNotMet has already been finalized where it was decided.
                if !matches!(e, AccordDriverError::ConditionNotMet { .. }) {
                    tracing::warn!(
                        txn_id = ?self.coordinator.txn_id,
                        error = %e,
                        "accord: transaction failed before Apply; finalizing it as a \
                         no-write so it cannot block later reads on its keys"
                    );
                    // The local replica is finalized before we return. The remote
                    // fan-out is not awaited: a replica holds its reply until the
                    // txn is Applied or its own bounded wait expires, and on this
                    // path the cluster is already slow or failing, so waiting
                    // would add that bound to every failed transaction's latency.
                    // Each remote failure is logged by the fan-out itself.
                    self.finalize_no_write_locally().await;
                    tokio::spawn(self.no_write_fanout());
                }
                return Err(e);
            }
        };
        self.apply_phase(commit_t, commit_deps).await
    }

    /// Phases 1-4 of [`Self::run_transaction`]: order the transaction
    /// (PreAccept, Accept, Commit) and gate it on its IF condition (read-vote).
    /// Returns the committed `(t, deps)` once the transaction may apply.
    async fn order_and_gate(&mut self) -> Result<(Timestamp, HashSet<TxnId>), AccordDriverError> {
        use crate::accord::wire::{
            AcceptOkPayload, AcceptPayload, CommitOkPayload, CommitPayload, LegacyAcceptOkPayload,
            PreAcceptOkPayload, PreAcceptPayload, PreAcceptV2Payload, ReadVoteOkPayload,
            ReadVotePayload,
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
        let pa_msg = if self.write_set.len() == 1 && self.snapshot_ts.is_none() {
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
                snapshot_ts: self.snapshot_ts,
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
        let postgres_snapshot_transaction = self.snapshot_ts.is_some()
            || matches!(
                self.read_predicate,
                crate::accord::wire::ReadPredicate::SnapshotBarrier
            );
        // Snapshot freshness is checked against the PostgreSQL marker key.
        // Multi-key transactions may span a union of replica sets, so the
        // transaction-wide quorum alone need not intersect the marker key's
        // committed quorum. Keep the fast path open until marker replicas have
        // also supplied a real slow quorum. With no per-key resolver (test and
        // legacy constructors), conservatively use the full transaction set.
        let snapshot_marker_replicas = self.snapshot_ts.map(|_| {
            let marker_key =
                ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_MARKER_KEY;
            self.per_key_replicas
                .as_ref()
                .map(|resolve| resolve(marker_key))
                .filter(|replicas| !replicas.is_empty())
                .unwrap_or_else(|| self.replica_ids.clone())
        });
        let snapshot_marker_quorum = snapshot_marker_replicas
            .as_ref()
            .map_or(0, |replicas| slow_quorum_size(replicas.len()));
        let mut snapshot_marker_votes = 0usize;
        if self_is_replica
            && !self.coordinator.is_leaseholder
            && (self_vote_cannot_short_circuit || postgres_snapshot_transaction)
        {
            if let Some(local_sm) = &self.local_accord_state {
                let keys: Vec<Vec<u8>> = self.write_set.iter().map(|w| w.key.clone()).collect();
                let snapshot_ts = self.snapshot_ts;
                let resp = crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                    let keys: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
                    sm.handle_preaccept_multi_with_snapshot(
                        txn_id,
                        t0,
                        &keys,
                        BallotNumber(0),
                        0,
                        snapshot_ts,
                    )
                })
                .await;
                if matches!(
                    resp.as_ref(),
                    Some(crate::accord::state_machine::SmResponse::SnapshotStale)
                ) {
                    return Err(AccordDriverError::SnapshotStale);
                }
                if let Some(crate::accord::state_machine::SmResponse::PreAcceptOK {
                    t, deps, ..
                }) = resp
                {
                    if snapshot_marker_replicas
                        .as_ref()
                        .is_some_and(|replicas| replicas.contains(&self.self_id))
                    {
                        snapshot_marker_votes += 1;
                    }
                    // At RF=2 the local vote would reach the fast quorum alone and
                    // suppress the remote PreAccept. PostgreSQL still registers the
                    // local conflict entry so COMMIT can validate the same snapshot
                    // marker on every replica, but it counts only the remote vote.
                    if self_vote_cannot_short_circuit {
                        decision = self.coordinator.handle_preaccept_ok(PreAcceptResponse {
                            from: self.coordinator.node_id,
                            t,
                            deps,
                        });
                    }
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
            use futures::StreamExt;

            let mut pending = futures::stream::FuturesUnordered::new();
            for &peer_id in self
                .replica_ids
                .iter()
                .filter(|&&peer_id| peer_id != self.self_id)
            {
                let peers = Arc::clone(&self.peers);
                let msg = pa_msg.clone();
                pending.push(async move {
                    let result = peers.send(peer_id, msg, Lane::Data).await;
                    if let Err(error) = &result {
                        tracing::warn!(
                            txn_id = ?txn_id,
                            error = %error,
                            peer = ?peer_id,
                            "accord: PreAccept RPC failed (non-fatal, continuing)"
                        );
                    }
                    (peer_id, result)
                });
            }
            let (response_tx, mut response_rx) =
                tokio::sync::mpsc::channel(self.replica_ids.len().max(1));
            tokio::spawn(async move {
                while let Some(response) = pending.next().await {
                    // A closed receiver means the coordinator already decided.
                    // Keep polling so every already-enqueued peer send is driven
                    // and its result can be observed/logged.
                    let _ = response_tx.send(response).await;
                }
            });

            // Count the outcomes so a failed round says WHY, not just that it
            // failed. An empty AccordPreAcceptOK is the replica's encoding for
            // a Nack, a persist failure, and an unexpected state alike, so
            // without this a quorum failure is indistinguishable from silence.
            let mut votes = 0usize;
            let mut no_votes = 0usize;
            let mut rpc_failures = 0usize;
            let mut response_count = 0usize;
            let mut phase_error = None;
            let preaccept_deadline = tokio::time::Instant::now() + self.preaccept_fast_path_timeout;
            let mut fast_path_deadline_elapsed = false;

            loop {
                let result = if fast_path_deadline_elapsed {
                    response_rx.recv().await
                } else {
                    match tokio::time::timeout_at(preaccept_deadline, response_rx.recv()).await {
                        Ok(result) => result,
                        Err(_) => {
                            fast_path_deadline_elapsed = true;
                            // A timeout is never a vote. It only closes the fast
                            // path window; ballot-1 Accept is safe once the normal
                            // slow quorum has supplied actual PreAccept votes.
                            if phase_error.is_none()
                                && self.coordinator.preaccept_response_count()
                                    >= slow_quorum_size(self.coordinator.rf)
                                && snapshot_marker_votes >= snapshot_marker_quorum
                            {
                                decision = self.coordinator.finalize_preaccept();
                                if matches!(decision, CoordinatorDecision::NeedAccept { .. }) {
                                    tracing::debug!(
                                        txn_id = ?txn_id,
                                        votes,
                                        timeout_ms = self.preaccept_fast_path_timeout.as_millis(),
                                        "accord: PreAccept fast-path window expired with a \
                                         slow quorum; falling back to Accept"
                                    );
                                    break;
                                }
                            }
                            continue;
                        }
                    }
                };
                let Some(result) = result else { break };
                response_count += 1;
                match result {
                    (_peer_id, Ok(Message::AccordPreAcceptOK(b))) if !b.is_empty() => {
                        let ok: PreAcceptOkPayload = match bincode::deserialize(&b) {
                            Ok(ok) => ok,
                            Err(error) => {
                                phase_error = Some(AccordDriverError::Codec(error.to_string()));
                                break;
                            }
                        };
                        if ok.snapshot_stale {
                            // The round is doomed. Stop here rather than wait on
                            // the rest of the fan-out: a paused replica would
                            // hold this dead txn registered on the live ones for
                            // its whole RPC timeout (see below).
                            phase_error = Some(AccordDriverError::SnapshotStale);
                            break;
                        }
                        if snapshot_marker_replicas
                            .as_ref()
                            .is_some_and(|replicas| replicas.contains(&_peer_id))
                        {
                            snapshot_marker_votes += 1;
                        }
                        votes += 1;
                        let resp = PreAcceptResponse {
                            from: ok.from,
                            t: ok.t,
                            deps: ok.deps,
                        };
                        let response_decision = self.coordinator.handle_preaccept_ok(resp);
                        if response_decision != CoordinatorDecision::Pending {
                            decision = response_decision;
                        }
                    }
                    (peer_id, Ok(_)) => {
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
                    (_peer_id, Err(_)) => {
                        rpc_failures += 1;
                    }
                }

                let snapshot_marker_quorum_reached =
                    snapshot_marker_votes >= snapshot_marker_quorum;
                if decision != CoordinatorDecision::Pending && snapshot_marker_quorum_reached {
                    // The coordinator has enough votes to decide this phase.
                    // The fanout task continues best-effort sends without
                    // keeping the transaction caller behind a slow minority.
                    break;
                }

                // If the window expired before enough valid votes arrived, keep
                // collecting. The first real response that completes the slow
                // quorum may enter Accept; a timeout never supplies that vote.
                if fast_path_deadline_elapsed
                    && phase_error.is_none()
                    && self.coordinator.preaccept_response_count()
                        >= slow_quorum_size(self.coordinator.rf)
                    && snapshot_marker_quorum_reached
                {
                    if decision == CoordinatorDecision::Pending {
                        decision = self.coordinator.finalize_preaccept();
                    }
                    if matches!(decision, CoordinatorDecision::NeedAccept { .. }) {
                        tracing::debug!(
                            txn_id = ?txn_id,
                            votes,
                            timeout_ms = self.preaccept_fast_path_timeout.as_millis(),
                            "accord: PreAccept slow quorum arrived after fast-path window; \
                             falling back to Accept"
                        );
                        break;
                    }
                    if decision != CoordinatorDecision::Pending {
                        break;
                    }
                }
            }

            if let Some(error) = phase_error {
                // Return now; the caller finalizes this txn as a no-write on
                // every replica at once. This used to drain every outstanding
                // PreAccept first, so that no late registration could race the
                // cleanup — and a paused replica made that drain last its full
                // 10 s RPC timeout, keeping the dead txn registered on the live
                // replicas. PostgreSQL snapshot barriers on its keys dep-waited
                // 5 s and failed (Jepsen fault schedule, 2026-09-29). The drain
                // now runs in the background instead, and finalizes each replica
                // whose PreAccept lands after this return.
                tokio::spawn(finalize_late_preaccept_registrations(
                    response_rx,
                    Arc::clone(&self.peers),
                    txn_id,
                ));
                return Err(error);
            }

            let snapshot_marker_quorum_reached = snapshot_marker_votes >= snapshot_marker_quorum;
            if snapshot_marker_replicas.is_some() && !snapshot_marker_quorum_reached {
                // A transaction-wide decision is insufficient if the marker-key
                // replica group did not supply its own slow quorum. In particular,
                // a small fast quorum for the union of keys must not bypass the
                // snapshot freshness check.
                tracing::warn!(
                    txn_id = ?txn_id,
                    marker_votes = snapshot_marker_votes,
                    marker_quorum = snapshot_marker_quorum,
                    "accord: snapshot PreAccept lacked a marker-key quorum"
                );
                decision = CoordinatorDecision::Pending;
            }

            // Every response that will ever arrive has arrived, so a fast
            // quorum is now provably unreachable. If a slow quorum voted, the
            // round commits through the Accept phase instead of stalling.
            if decision == CoordinatorDecision::Pending && snapshot_marker_quorum_reached {
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
                    peers = response_count,
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
                        let deps = accept_deps.clone();
                        let resp = crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                            sm.handle_accept(txn_id, t0, t, deps, BallotNumber(1))
                        })
                        .await;
                        if let Some(crate::accord::state_machine::SmResponse::AcceptOK {
                            deps: effective_deps,
                            ..
                        }) = resp
                        {
                            ac_decision = self.coordinator.handle_accept_ok(AcceptResponse {
                                from: self.coordinator.node_id,
                                ballot: BallotNumber(1),
                                deps: effective_deps,
                            });
                        }
                    }
                }

                if ac_decision == CoordinatorDecision::Pending {
                    use futures::StreamExt;

                    let mut ac_pending = futures::stream::FuturesUnordered::new();
                    for &peer_id in self
                        .replica_ids
                        .iter()
                        .filter(|&&peer_id| peer_id != self.self_id)
                    {
                        let peers = Arc::clone(&self.peers);
                        let msg = ac_msg.clone();
                        ac_pending.push(async move {
                            let result = peers.send(peer_id, msg, Lane::Data).await;
                            if let Err(error) = &result {
                                tracing::warn!(
                                    txn_id = ?txn_id,
                                    error = %error,
                                    peer = ?peer_id,
                                    "accord: Accept RPC failed"
                                );
                            }
                            (peer_id, result)
                        });
                    }
                    let (accept_tx, mut accept_rx) =
                        tokio::sync::mpsc::channel(self.replica_ids.len().max(1));
                    tokio::spawn(async move {
                        while let Some(response) = ac_pending.next().await {
                            let _ = accept_tx.send(response).await;
                        }
                    });

                    let mut accept_error = None;
                    while let Some((peer_id, result)) = accept_rx.recv().await {
                        if accept_error.is_some() {
                            continue;
                        }
                        match result {
                            Ok(Message::AccordAcceptOK(b)) if !b.is_empty() => {
                                let ok: AcceptOkPayload = match bincode::deserialize(&b) {
                                    Ok(ok) => ok,
                                    Err(_) => {
                                        // Older replicas only echo txn_id. Their Accept
                                        // request carried this coordinator's dependency
                                        // set, so retain that as the compatibility value.
                                        let legacy: LegacyAcceptOkPayload =
                                            match bincode::deserialize(&b) {
                                                Ok(legacy) => legacy,
                                                Err(error) => {
                                                    accept_error = Some(AccordDriverError::Codec(
                                                        error.to_string(),
                                                    ));
                                                    continue;
                                                }
                                            };
                                        AcceptOkPayload {
                                            txn_id: legacy.txn_id,
                                            deps: accept_deps.clone(),
                                        }
                                    }
                                };
                                let resp = AcceptResponse {
                                    // Use the RPC peer identity as the voter. The
                                    // transaction ID's node field identifies its
                                    // coordinator and is shared by every AcceptOK.
                                    from: u64::from_be_bytes(
                                        peer_id.as_bytes()[..8]
                                            .try_into()
                                            .expect("UUID has 16 bytes"),
                                    ),
                                    ballot: BallotNumber(1),
                                    deps: ok.deps,
                                };
                                ac_decision = self.coordinator.handle_accept_ok(resp);
                            }
                            Ok(_) => {}
                            Err(_) => {}
                        }
                        if ac_decision != CoordinatorDecision::Pending {
                            // The fanout task continues best-effort sends after
                            // this slow quorum is sufficient for Accept.
                            break;
                        }
                    }
                    if let Some(error) = accept_error {
                        // Drain already-sent Accepts before no-write
                        // finalization to avoid racing a late accepted state.
                        return Err(error);
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

        // A coordinator that is itself a replica must durably process its own
        // Commit before it may count itself toward the commit quorum or serve
        // the following read-vote. Treating self as an implicit ack without
        // updating the local state leaves one replica blind to its own txn; two
        // concurrent IF NOT EXISTS coordinators can then each see only one false
        // vote and both apply. An unpublished state is therefore a hard wiring
        // error, and a local fsync failure is a failed commit vote.
        if self_is_replica {
            let local_sm = self.local_accord_state.as_ref().ok_or_else(|| {
                AccordDriverError::Network(
                    "coordinator is a replica but its local Accord state is unpublished".into(),
                )
            })?;
            let deps: Vec<TxnId> = commit_deps.iter().copied().collect();
            let locally_committed =
                crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                    sm.handle_commit(txn_id, t0, commit_t, deps);
                    sm.get_state(&txn_id)
                        .map(|state| matches!(state.phase, TxnPhase::Committed | TxnPhase::Applied))
                        .unwrap_or(false)
                })
                .await
                .unwrap_or(false);
            if !locally_committed {
                return Err(AccordDriverError::Network(
                    "coordinator local Accord commit was not durably recorded".into(),
                ));
            }
        }

        // Per-shard quorum: every shard the write-set touches must independently
        // reach its slow quorum. A single global counter would let one shard
        // commit while another is a minority — the cross-shard non-atomicity
        // Accord exists to prevent. `participant_set` resolves per-key replica
        // sets when a multi-shard resolver is wired, else collapses to one shard
        // (the behavior-preserving single-key / single-replica-set default).
        let participant = self.participant_set();
        let commit_txn = txn_id;
        let is_commit_ok = move |r: &ferrosa_net::error::Result<Message>| {
            matches!(r, Ok(Message::AccordCommit(b))
                if bincode::deserialize::<CommitOkPayload>(b)
                    .map(|ok| ok.txn_id == commit_txn)
                    .unwrap_or(false))
        };
        if !self
            .quorum_broadcast(commit_msg, &participant, is_commit_ok)
            .await
        {
            return Err(AccordDriverError::QuorumUnavailable);
        }

        // debug!, not info!: this fires once per transaction. At INFO it was 70%
        // of a 1.6 GB unrotated log, and those writes saturated the disk the CQL
        // runtime needs -- which is what stopped this same code answering read
        // votes. Guarded by tests/consensus_logging_is_bounded.rs.
        tracing::debug!(
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

            let mut votes_true = 0usize;
            let mut votes_false = 0usize;
            let mut local_snapshot_vote = false;
            let mut dissenting_row: Vec<u8> = Vec::new();
            // For the generic ReadRow predicate: collect each replica's row-at-`t`
            // bytes so we can require F+1 *agreement* on the row state before the
            // coordinator evaluates the IF predicate. Disagreement is a correctness
            // failure (non-linearizable read) and must abort, never silently pick one.
            let mut read_rows: Vec<Vec<u8>> = Vec::new();
            let is_generic = self.read_predicate.row_read().is_some();
            let is_snapshot_barrier = matches!(
                self.read_predicate,
                crate::accord::wire::ReadPredicate::SnapshotBarrier
            );

            // The coordinator's own replica is not reachable over the network
            // (self-send fails). For the generic path it must contribute its local
            // read-at-`t` so that, with RF=2 (sq=2), F+1 agreement is achievable and
            // the result is deterministic across all replicas. The applier already
            // persisted earlier conflicting txns locally before this read (dep-wait).
            // Only a replica of the key may vote on its row. A coordinator that
            // does not own the key (RF below the cluster size) holds no copy of
            // it, so its local read is always "absent" -- and at RF=1 that one
            // empty read WAS the F+1 agreement: a conditional UPDATE never saw a
            // row the replica held, and INSERT IF NOT EXISTS applied over it
            // (t_0bcd56f7). The same `self_is_replica` gate the PreAccept phase
            // uses.
            let self_is_replica =
                self_id != uuid::Uuid::nil() && self.replica_ids.contains(&self_id);
            if is_snapshot_barrier {
                if let Some(local_sm) = &self.local_accord_state {
                    if crate::accord::handlers::await_conflicting_deps_applied(
                        local_sm, &key, commit_t,
                    )
                    .await
                    {
                        votes_true += 1;
                        local_snapshot_vote = true;
                    } else {
                        tracing::error!(
                            txn_id = ?txn_id,
                            "accord: coordinator local snapshot barrier timed out — abstaining"
                        );
                    }
                }
            } else if is_generic && self_is_replica {
                if let Some(read) = self.read_predicate.row_read() {
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
                            let predicate = self.read_predicate.clone();
                            let k = key.clone();
                            let row =
                                crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                                    let read = predicate
                                        .row_read()
                                        .expect("a row-reading predicate names its row");
                                    sm.read_row_bytes_at(read, &k, commit_t)
                                })
                                .await;
                            // `None` is a cancelled read (logged): abstain.
                            if let Some(row) = row {
                                read_rows.push(row.unwrap_or_default());
                            }
                        } else {
                            tracing::error!(
                                txn_id = ?txn_id,
                                "accord: coordinator local read-vote dep-wait timed out — abstaining"
                            );
                            // Abstain: contribute no local read. F+1 agreement then
                            // fails loud below rather than treating a stale read as truth.
                        }
                    } else if let Some(reader) = &self.local_reader {
                        match reader.read_for(read, &key, commit_t) {
                            Ok(bytes) => read_rows.push(bytes.unwrap_or_default()),
                            Err(e) => {
                                return Err(AccordDriverError::Network(format!(
                                    "coordinator local read-at-t failed: {e}"
                                )));
                            }
                        }
                    }
                }
            } else if !is_generic {
                // `NotExists`: no table or clustering to read, and the replicas
                // refuse it too (t_fe2426bb). The local replica abstains, so the
                // vote cannot reach F+1 and the transaction fails loud.
                tracing::error!(
                    txn_id = ?txn_id,
                    "accord: NotExists read predicate has no row to read — the coordinator \
                     must send ReadRow; abstaining"
                );
            }

            let remote_read_futs: Vec<_> = if is_snapshot_barrier {
                // Commit already proves the slow quorum for a snapshot barrier.
                // The local dep-wait below proves this engine has applied prior
                // conflicts; waiting on remote ReadVotes would make an unavailable
                // minority delay PostgreSQL snapshot creation.
                Vec::new()
            } else {
                self.replica_ids
                    .iter()
                    .filter(|&&id| id != self_id)
                    .map(|&peer_id| {
                        let peers = Arc::clone(&self.peers);
                        let msg = read_msg.clone();
                        async move { peers.send(peer_id, msg, Lane::Data).await }
                    })
                    .collect()
            };
            collect_until_decided(remote_read_futs, |result| {
                match result {
                    Ok(Message::AccordReadOK(b)) if !b.is_empty() => {
                        match bincode::deserialize::<ReadVoteOkPayload>(b) {
                            Ok(vote) => {
                                if is_generic {
                                    // Generic IF: collect row bytes until F+1 matching
                                    // reads decide the result. A lagging minority must
                                    // not hold the read open after agreement is reached.
                                    read_rows.push(vote.current_row.clone());
                                } else if vote.condition_holds {
                                    votes_true += 1;
                                } else {
                                    votes_false += 1;
                                    if dissenting_row.is_empty() {
                                        dissenting_row = vote.current_row.clone();
                                    }
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    txn_id = ?txn_id,
                                    error = %error,
                                    "accord: could not decode ReadVote response; skipping malformed vote"
                                );
                            }
                        }
                    }
                    Ok(response) => {
                        tracing::warn!(
                            txn_id = ?txn_id,
                            response_type = ?response.msg_type(),
                            "accord: unexpected ReadVote response; skipping it"
                        );
                    }
                    Err(e) => {
                        // No response or network error — skip (don't count as false).
                        tracing::warn!(
                            txn_id = ?txn_id,
                            error = %e,
                            "accord: ReadVote RPC failed (non-fatal)"
                        );
                    }
                }

                if is_generic {
                    agreed_row(&read_rows, sq).is_some()
                } else {
                    decide_existence_votes(votes_true, votes_false, sq)
                        != ExistenceVoteDecision::QuorumUnavailable
                }
            })
            .await;

            // A successful Commit phase already proves the Accord slow quorum.
            // This local dep-wait is the remaining snapshot condition: the node
            // serving this session must not expose an older engine view. Remote
            // ReadVotes are still collected for the shared read path, but their
            // availability cannot block a snapshot on this coordinator.
            if is_snapshot_barrier && !local_snapshot_vote {
                return Err(AccordDriverError::Network(
                    "PostgreSQL snapshot barrier dependencies were not applied locally".into(),
                ));
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
                        tracing::debug!(
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
            } else if !is_snapshot_barrier {
                // F+1 matching votes decide BOTH outcomes. The legacy path only
                // required a false quorum and treated every other shape — even
                // zero replies — as permission to apply.
                match decide_existence_votes(votes_true, votes_false, sq) {
                    ExistenceVoteDecision::Apply => {}
                    ExistenceVoteDecision::ConditionNotMet => {
                        tracing::debug!(
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
                    ExistenceVoteDecision::QuorumUnavailable => {
                        tracing::error!(
                            txn_id = ?txn_id,
                            votes_true,
                            votes_false,
                            required = sq,
                            "accord: existence read-vote lacked an explicit F+1 decision"
                        );
                        return Err(AccordDriverError::QuorumUnavailable);
                    }
                }
            }
        } // end read-vote phase (skipped for ReadPredicate::Always)

        Ok((commit_t, commit_deps))
    }

    /// Phase 5 of [`Self::run_transaction`]: apply the committed transaction
    /// locally and on the remote replicas, and wait for the Apply quorum.
    /// Apply phase with the operator-tunable bound (see [`configured_txn_timeout`]).
    async fn apply_phase(
        &mut self,
        commit_t: Timestamp,
        commit_deps: HashSet<TxnId>,
    ) -> Result<(Timestamp, HashSet<TxnId>), AccordDriverError> {
        self.apply_phase_within(
            commit_t,
            commit_deps,
            crate::accord::state_machine::configured_txn_timeout(),
        )
        .await
    }

    /// Apply phase with an explicit dependency-wait bound.
    ///
    /// The bound is a parameter so the abandon path is testable:
    /// `configured_txn_timeout` resolves the environment once and caches it
    /// process-wide, so a test cannot move the production 10 s bound.
    async fn apply_phase_within(
        &mut self,
        commit_t: Timestamp,
        commit_deps: HashSet<TxnId>,
        bound: std::time::Duration,
    ) -> Result<(Timestamp, HashSet<TxnId>), AccordDriverError> {
        use crate::accord::wire::ApplyOkPayload;

        let txn_id = self.coordinator.txn_id;
        let self_id = self.self_id;
        let self_is_replica = self.replica_ids.contains(&self_id) && self_id != uuid::Uuid::nil();
        let participant = self.participant_set();

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
                .filter(|e| self.replica_owns_key(self_id, &e.key))
                .map(|e| e.mutation.clone())
                .collect();
            if let Some(local_sm) = &self.local_accord_state {
                crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                    sm.handle_apply_writeset(txn_id, owned_writes)
                })
                .await;
            } else if !owned_writes.is_empty() {
                if let Some(applier) = &self.local_applier {
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
        // `participant` built above). An Apply ack is an `AccordApplyOK` whose
        // payload's `txn_id` matches THIS transaction. An empty body or an
        // unparseable payload is NOT an ack: a bare ApplyOK proves nothing about
        // which transaction (if any) the peer applied, so it must never count
        // toward the quorum. This is a safety check, not an affordance — the
        // only senders that ever emitted a bare ApplyOK were test doubles.
        let local_state = if self_is_replica {
            self.local_accord_state.clone()
        } else {
            None
        };
        let local_apply_wait = async move {
            match local_state {
                Some(state) => {
                    crate::accord::handlers::await_txn_applied_within(&state, txn_id, bound).await
                }
                None => true,
            }
        };

        // Single-key keeps the v1 `AccordApply` wire (byte-identical). Multi-key
        // sends each replica a per-replica `AccordApplyV2` scoped to the keys it
        // owns, so a replica never persists a key it is not a replica for. Fanout
        // runs alongside the local dependency wait: a parked coordinator replica
        // must not suppress Apply propagation to the other replicas.
        let remote_apply = async {
            let apply_txn = txn_id;
            let is_apply_ok = move |r: &ferrosa_net::error::Result<Message>| {
                matches!(r, Ok(Message::AccordApplyOK(b))
                    if bincode::deserialize::<ApplyOkPayload>(b)
                        .map(|ok| ok.txn_id == apply_txn)
                        .unwrap_or(false))
            };
            if self.write_set.len() == 1 {
                let apply_bytes = self.apply_payload_bytes()?;
                let apply_msg = Message::AccordApply(Bytes::from(apply_bytes));
                Ok::<bool, AccordDriverError>(
                    self.quorum_broadcast(apply_msg, &participant, is_apply_ok)
                        .await,
                )
            } else {
                let per_peer = self.apply_v2_messages()?;
                Ok(self
                    .quorum_broadcast_per_peer(
                        &participant,
                        |peer_id| {
                            per_peer
                                .get(&peer_id)
                                .cloned()
                                .expect("every replica_id has a per-peer AccordApplyV2 message")
                        },
                        is_apply_ok,
                    )
                    .await)
            }
        };
        let (local_applied, apply_result) = tokio::join!(local_apply_wait, remote_apply);

        if !local_applied {
            let state = if let Some(local_sm) = &self.local_accord_state {
                crate::accord::handlers::on_state_machine(local_sm, move |sm| {
                    sm.get_state(&txn_id).map(|txn| {
                        let dependencies = txn
                            .deps
                            .iter()
                            .map(|dependency| {
                                (
                                    *dependency,
                                    sm.get_state(dependency).map(|state| state.phase),
                                )
                            })
                            .collect::<Vec<_>>();
                        (
                            txn.phase,
                            dependencies,
                            txn.result.as_ref().map_or(0, Vec::len),
                        )
                    })
                })
                .await
            } else {
                None
            };
            let timeout = bound;
            tracing::error!(
                txn_id = ?txn_id,
                ?state,
                owned_write_count = self.write_set.iter().filter(|entry| self.replica_owns_key(self_id, &entry.key)).count(),
                ?timeout,
                "accord: coordinator local Apply did not reach Applied within the dependency-wait \
                 bound — abandoning the transaction (NOT committed; safe to retry)"
            );
            // The LOCAL replica is the one this coordinator must be able to serve
            // from, and its Apply could not resolve its dependencies inside the
            // same bound the remote quorum uses. That is the SAME condition as a
            // failed remote apply quorum, so it must take the same path: roll the
            // transaction back, release anything parked behind it, and tell the
            // client it did not commit so it can retry.
            //
            // Returning a bare `Network(..)` here — which is what this did before —
            // bypassed the abandon entirely. The client then received an opaque,
            // non-retryable failure for a transaction that had never been applied,
            // which is exactly what stalled the PostgreSQL front end and what the
            // Jepsen workload could not classify as retryable.
            self.finalize_no_write().await;
            return Err(AccordDriverError::TxnAbandoned { timeout });
        }
        let apply_ok = apply_result?;
        if !apply_ok {
            // The bounded dependency wait expired: abandon the transaction.
            //
            // Every replica that parked refused its ApplyOK — it never applied —
            // so nothing durable exists and rolling the transaction back is the
            // true outcome. Finalizing it as a no-write additionally releases any
            // successor parked behind it, so one slow apply can never poison the
            // key the way an un-finalized Committed entry did. The client is told
            // it did not commit and may retry.
            let timeout = bound;
            tracing::error!(
                txn_id = ?txn_id,
                unmet = ?participant.shards.len(),
                ?timeout,
                "accord: Apply quorum not reached within the dependency-wait bound — \
                 abandoning the transaction (NOT committed; safe to retry)"
            );
            self.finalize_no_write().await;
            return Err(AccordDriverError::TxnAbandoned { timeout });
        }

        tracing::debug!(
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
        use futures::StreamExt;

        let self_id = self.self_id;
        let mut quorum = participant.quorum();
        if self.replica_ids.contains(&self_id) && self_id != uuid::Uuid::nil() {
            quorum.record_node_ack(self_id);
        }

        let txn_id = self.coordinator.txn_id;
        if quorum.all_reached() {
            return true;
        }
        let mut pending = futures::stream::FuturesUnordered::new();
        for &peer_id in self.replica_ids.iter().filter(|&&id| id != self_id) {
            let peers = Arc::clone(&self.peers);
            let msg = build_msg(peer_id);
            pending.push(async move {
                let result = peers.send(peer_id, msg, Lane::Data).await;
                if let Err(error) = &result {
                    tracing::warn!(
                        txn_id = ?txn_id,
                        error = %error,
                        peer = ?peer_id,
                        "accord: quorum broadcast RPC failed"
                    );
                }
                (peer_id, result)
            });
        }
        let (response_tx, mut response_rx) =
            tokio::sync::mpsc::channel(self.replica_ids.len().max(1));
        tokio::spawn(async move {
            while let Some(response) = pending.next().await {
                let _ = response_tx.send(response).await;
            }
        });

        while let Some((peer_id, result)) = response_rx.recv().await {
            if is_ack(&result) {
                quorum.record_node_ack(peer_id);
                if quorum.all_reached() {
                    // A quorum makes this phase durable. The fanout task keeps
                    // sending to remaining peers without holding the PostgreSQL
                    // session on a slow or unavailable minority replica.
                    return true;
                }
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

/// Drain the PreAccept fan-out of a transaction that already failed and
/// no-write-finalize every replica whose PreAccept registered it late.
///
/// The coordinator returns as soon as the round is doomed, and its caller
/// finalizes every replica at once. A PreAccept still in flight can land on a
/// slow replica AFTER that finalize and register the txn there again, which
/// would leave it a pending conflict forever (FMEA CL-20). Each such late
/// registration answers here, so it gets its own finalize.
async fn finalize_late_preaccept_registrations(
    mut responses: tokio::sync::mpsc::Receiver<(uuid::Uuid, ferrosa_net::error::Result<Message>)>,
    peers: Arc<dyn crate::accord::transport::AccordTransport>,
    txn_id: TxnId,
) {
    use crate::accord::wire::ApplyPayload;
    let payload = ApplyPayload {
        txn_id,
        result_data: Vec::new(),
    };
    let msg = match bincode::serialize(&payload) {
        Ok(bytes) => Message::AccordApply(Bytes::from(bytes)),
        Err(e) => {
            tracing::error!(txn_id = ?txn_id, error = %e, "accord: encode late no-write finalize failed");
            return;
        }
    };
    while let Some((peer, result)) = responses.recv().await {
        let registered = matches!(&result, Ok(Message::AccordPreAcceptOK(b)) if !b.is_empty());
        if !registered {
            continue;
        }
        let undelivered =
            deliver_no_write_finalize(&peers, vec![peer], &msg, txn_id, NO_WRITE_FINALIZE_RETRY)
                .await;
        if !undelivered.is_empty() {
            tracing::error!(
                txn_id = ?txn_id,
                peer = %peer,
                "accord: a late PreAccept registered a failed transaction and its no-write \
                 finalize never reached the replica; reads and snapshot barriers on its keys \
                 there will dep-wait and abstain until it is recovered"
            );
        }
    }
}

/// How hard the no-write finalize tries to reach each replica.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FinalizeRetry {
    pub(crate) attempts: u32,
    pub(crate) first_backoff: std::time::Duration,
    pub(crate) max_backoff: std::time::Duration,
}

/// About 25 s in total: long enough to ride out a paused or overloaded
/// replica, bounded so a dead one does not hold the task forever.
pub(crate) const NO_WRITE_FINALIZE_RETRY: FinalizeRetry = FinalizeRetry {
    attempts: 8,
    first_backoff: std::time::Duration::from_millis(200),
    max_backoff: std::time::Duration::from_secs(5),
};

/// Send the no-write finalize to every replica in `remotes`, retrying each
/// one with capped exponential backoff until it acks or `retry.attempts` run
/// out. Returns the replicas that never acked.
///
/// A replica that misses the finalize keeps the failed transaction as a
/// pending conflict on its keys; every later read or snapshot barrier there
/// dep-waits on it and abstains. It was sent once; on 2026-09-28 a timed-out
/// send left a live replica blocked and failed a Jepsen snapshot barrier.
///
/// Logs the edges per replica: the first failure (with the peer) and a later
/// recovery, not every attempt.
pub(crate) async fn deliver_no_write_finalize(
    peers: &Arc<dyn crate::accord::transport::AccordTransport>,
    remotes: Vec<uuid::Uuid>,
    msg: &Message,
    txn_id: TxnId,
    retry: FinalizeRetry,
) -> Vec<uuid::Uuid> {
    let per_replica = remotes.into_iter().map(|peer_id| {
        let peers = Arc::clone(peers);
        let msg = msg.clone();
        async move {
            let mut backoff = retry.first_backoff;
            for attempt in 1..=retry.attempts {
                match peers.send(peer_id, msg.clone(), Lane::Data).await {
                    Ok(_) => {
                        if attempt > 1 {
                            // debug: per-transaction, and the hot-path log rule
                            // (tests/consensus_logging_is_bounded.rs) forbids
                            // INFO with a txn_id here.
                            tracing::debug!(
                                txn_id = ?txn_id, peer = %peer_id, attempt,
                                "accord: no-write finalize reached the replica after retrying"
                            );
                        }
                        return None;
                    }
                    Err(e) => {
                        if attempt == 1 {
                            tracing::warn!(
                                txn_id = ?txn_id, peer = %peer_id, error = %e,
                                "accord: no-write finalize RPC failed; retrying with backoff"
                            );
                        }
                        if attempt < retry.attempts {
                            tokio::time::sleep(backoff).await;
                            backoff = (backoff * 2).min(retry.max_backoff);
                        }
                    }
                }
            }
            Some(peer_id)
        }
    });
    futures::future::join_all(per_replica)
        .await
        .into_iter()
        .flatten()
        .collect()
}

#[cfg(test)]
mod tests {

    /// The abandoned-transaction error must be distinguishable BY THE CLIENT.
    ///
    /// The committer erases this typed error to a `reason: String` before the
    /// PostgreSQL front end sees it, so the front end classifies by prefix. If the
    /// prefix drifts, an abandoned (retryable) transaction is reported as an
    /// opaque 58000 fault and a client cannot tell "retry me" from "broken".
    #[test]
    fn an_abandoned_transaction_carries_the_client_signal() {
        let rendered = AccordDriverError::TxnAbandoned {
            timeout: std::time::Duration::from_secs(5),
        }
        .to_string();
        assert!(
            rendered.starts_with("abandoned:"),
            "the `abandoned:` prefix is the client contract; got {rendered:?}"
        );
        assert!(
            rendered.contains("NOT committed"),
            "the client must be told the transaction did not commit; got {rendered:?}"
        );
    }

    /// Control: no OTHER apply failure may carry the retryable prefix, or every
    /// quorum error would be reported to the client as safe to retry — the exact
    /// opposite of telling it the truth.
    #[test]
    fn other_driver_errors_do_not_carry_the_abandoned_prefix() {
        for error in [
            AccordDriverError::QuorumUnavailable,
            AccordDriverError::SnapshotStale,
        ] {
            assert!(
                !error.to_string().starts_with("abandoned:"),
                "{error} must not be advertised as retryable"
            );
        }
    }

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

    /// t_b986c335: during a rolling upgrade an old replica votes the row with
    /// its stored nanosecond stamps and an upgraded one with the same stamps
    /// normalised to microseconds. Same row, different bytes: agreement must
    /// compare the decoded row, not the encoding, or every LWT on a legacy row
    /// fails F+1 until the roll finishes.
    #[test]
    fn two_encodings_of_the_same_row_agree() {
        use ferrosa_common::{CellValue, DecoratedKey, PartitionKey};
        use ferrosa_sstable::types::{DeletionTime, LivenessInfo, Row};
        use ferrosa_storage::Mutation;

        let vote = |cell_ts: i64, mutation_ts: i64| {
            let m = Mutation {
                mutation_id: [0u8; 16],
                keyspace: "ks".to_string(),
                table: "t".to_string(),
                key: DecoratedKey::new(PartitionKey::new(b"k".to_vec())),
                rows: vec![Row {
                    clustering: vec![],
                    cells: vec![(0, CellValue::live(b"v".to_vec(), cell_ts))],
                    deletion: DeletionTime::LIVE,
                    primary_key_liveness: LivenessInfo::with_timestamp(cell_ts),
                }],
                timestamp: mutation_ts,
            };
            let mut buf = vec![0u8; m.serialized_size()];
            m.serialize_into(&mut buf);
            buf
        };
        let (cell_us, t_ns) = (1_791_437_153_001_234_i64, 1_791_437_160_000_000_456_i64);
        let old_replica = vote(cell_us * 1_000 + 789, t_ns);
        let new_replica = vote(cell_us, t_ns / 1_000);
        assert_ne!(old_replica, new_replica, "the encodings differ");

        let agreed = agreed_row(&[old_replica, new_replica.clone()], 2)
            .expect("the same row from both replicas is F+1 agreement");
        let m = Mutation::deserialize_from(&agreed).unwrap();
        assert_eq!(m.rows[0].cells[0].1.timestamp, cell_us);
        assert_eq!(
            agreed, new_replica,
            "the agreed row is the canonical encoding"
        );
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

    /// One affirmative vote at RF=3 is not F+1. A timeout or abstention from the
    /// other replicas must fail closed, never turn "unknown" into permission to
    /// apply a conditional write.
    #[test]
    fn existence_vote_without_true_quorum_is_unavailable() {
        assert_eq!(
            decide_existence_votes(1, 0, 2),
            ExistenceVoteDecision::QuorumUnavailable
        );
        assert_eq!(
            decide_existence_votes(0, 1, 2),
            ExistenceVoteDecision::QuorumUnavailable
        );
        assert_eq!(
            decide_existence_votes(0, 0, 2),
            ExistenceVoteDecision::QuorumUnavailable
        );
    }

    #[test]
    fn existence_vote_requires_an_explicit_quorum_decision() {
        assert_eq!(
            decide_existence_votes(2, 0, 2),
            ExistenceVoteDecision::Apply
        );
        assert_eq!(
            decide_existence_votes(1, 2, 2),
            ExistenceVoteDecision::ConditionNotMet
        );
    }
    use super::*;

    #[tokio::test]
    async fn read_vote_collection_stops_after_quorum_without_waiting_for_slow_replica() {
        use std::future::Future;
        use std::pin::Pin;

        let responses: Vec<Pin<Box<dyn Future<Output = bool>>>> = vec![
            Box::pin(async { true }),
            Box::pin(async { true }),
            Box::pin(std::future::pending()),
        ];
        let mut votes = 0;

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            collect_until_decided(responses, |vote| {
                votes += usize::from(*vote);
                votes >= 2
            }),
        )
        .await;

        assert!(
            result.is_ok(),
            "a slow minority replica must not delay quorum"
        );
        assert_eq!(votes, 2);
    }
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
    fn late_preaccept_ok_after_the_round_left_preaccepting_is_ignored_not_panicked() {
        // RF=3, coordinator node 1, non-leaseholder. Two replicas answer (a slow
        // quorum), both agreeing on t0, so the round cannot complete on the fast
        // path and `finalize_preaccept` moves it to Accepting.
        let t0 = make_ts(1000);
        let txn_id = make_txn_id(1, 1000);
        let mut coord = AccordCoordinator::new(txn_id, t0, b"key1".to_vec(), 1, 3, false);

        assert_eq!(
            coord.handle_preaccept_ok(PreAcceptResponse {
                from: 1,
                t: t0,
                deps: vec![]
            }),
            CoordinatorDecision::Pending
        );
        assert_eq!(
            coord.handle_preaccept_ok(PreAcceptResponse {
                from: 2,
                t: t0,
                deps: vec![]
            }),
            CoordinatorDecision::Pending
        );
        assert!(matches!(
            coord.finalize_preaccept(),
            CoordinatorDecision::NeedAccept { .. }
        ));
        assert_eq!(coord.phase, CoordinatorPhase::Accepting);

        // A late PreAcceptOK now arrives from a replica that had not voted. The
        // round has left PreAccepting, so this vote may no longer influence the
        // decision. It must be IGNORED, not panic the coordinator thread — the
        // panic (`handle_preaccept_ok called in wrong phase`) killed the writer
        // task, which surfaced as `Accord apply quorum unavailable` and a lost
        // PostgreSQL connection.
        assert_eq!(
            coord.handle_preaccept_ok(PreAcceptResponse {
                from: 3,
                t: t0,
                deps: vec![]
            }),
            CoordinatorDecision::Pending
        );
        assert_eq!(coord.phase, CoordinatorPhase::Accepting);
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
        // A replica may have discovered an additional conflict after PreAccept.
        // The accepted quorum must carry that dependency forward to Commit.
        let accept_only_dep = make_txn_id(3, 750);
        let a1 = coord.handle_accept_ok(AcceptResponse {
            from: 1,
            ballot: BallotNumber(1),
            deps: vec![other_txn],
        });
        assert_eq!(a1, CoordinatorDecision::Pending);

        let a2 = coord.handle_accept_ok(AcceptResponse {
            from: 2,
            ballot: BallotNumber(1),
            deps: vec![other_txn, accept_only_dep],
        });
        match a2 {
            CoordinatorDecision::SlowPathCommit { deps, .. } => {
                assert!(deps.contains(&other_txn));
                assert!(
                    deps.contains(&accept_only_dep),
                    "Accept quorum dependency was lost"
                );
            }
            other => panic!("expected SlowPathCommit, got {other:?}"),
        }
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
        slow: std::collections::HashSet<uuid::Uuid>,
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
            if self.slow.contains(&host_id) {
                std::future::pending::<()>().await;
            }
            if *self.behavior.get(&host_id).unwrap_or(&false) {
                Ok(self.ok.clone())
            } else {
                Err(ferrosa_net::error::NetError::Timeout(
                    "mock node down".into(),
                ))
            }
        }
    }

    /// Fails the first `fail_first[peer]` sends to each peer, then acks.
    /// Counts every send per peer.
    struct FlakyTransport {
        fail_first: std::collections::HashMap<uuid::Uuid, usize>,
        sends: std::sync::Mutex<std::collections::HashMap<uuid::Uuid, usize>>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for FlakyTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            _msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            let attempt = {
                let mut sends = self.sends.lock().unwrap();
                let n = sends.entry(host_id).or_insert(0);
                *n += 1;
                *n
            };
            if attempt <= *self.fail_first.get(&host_id).unwrap_or(&0) {
                Err(ferrosa_net::error::NetError::Timeout("flaky".into()))
            } else {
                Ok(Message::AccordApply(Bytes::new()))
            }
        }
    }

    fn flaky(fail_first: &[(u128, usize)]) -> Arc<FlakyTransport> {
        Arc::new(FlakyTransport {
            fail_first: fail_first
                .iter()
                .map(|&(p, n)| (uuid::Uuid::from_u128(p), n))
                .collect(),
            sends: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// 2026-09-28 Jepsen: a failed transaction's no-write finalize was sent
    /// once; the send to a live replica timed out, and that replica kept the
    /// transaction as a pending conflict until a later snapshot barrier on the
    /// key waited 5 s and failed. A replica that fails is retried until it acks.
    #[tokio::test(start_paused = true)]
    async fn no_write_finalize_retries_a_replica_until_it_acks() {
        let transport = flaky(&[(2, 3), (3, 0)]);
        let peers: Arc<dyn AccordTransport> = transport.clone();
        let undelivered = deliver_no_write_finalize(
            &peers,
            vec![uuid::Uuid::from_u128(2), uuid::Uuid::from_u128(3)],
            &Message::AccordApply(Bytes::new()),
            TxnId::new(1, make_ts(1)),
            NO_WRITE_FINALIZE_RETRY,
        )
        .await;
        assert!(
            undelivered.is_empty(),
            "every replica acked: {undelivered:?}"
        );
        let sends = transport.sends.lock().unwrap();
        assert_eq!(
            sends[&uuid::Uuid::from_u128(2)],
            4,
            "three failures, then the ack"
        );
        assert_eq!(
            sends[&uuid::Uuid::from_u128(3)],
            1,
            "a healthy replica is sent once"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_write_finalize_gives_up_after_bounded_attempts_and_names_the_replica() {
        let transport = flaky(&[(2, usize::MAX)]);
        let peers: Arc<dyn AccordTransport> = transport.clone();
        let undelivered = deliver_no_write_finalize(
            &peers,
            vec![uuid::Uuid::from_u128(2)],
            &Message::AccordApply(Bytes::new()),
            TxnId::new(1, make_ts(1)),
            NO_WRITE_FINALIZE_RETRY,
        )
        .await;
        assert_eq!(undelivered, vec![uuid::Uuid::from_u128(2)]);
        assert_eq!(
            transport.sends.lock().unwrap()[&uuid::Uuid::from_u128(2)],
            NO_WRITE_FINALIZE_RETRY.attempts as usize,
            "attempts are bounded"
        );
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

    /// A write-set larger than the node's conflict-index capacity can never be
    /// decided: a PreAccept registers the transaction under EVERY key and is
    /// all-or-nothing, so every replica refuses it and the coordinator would
    /// report the opaque "Accord quorum unavailable" that a ~1.1M-row
    /// transactional COPY produced live on 2026-10-10 (`key_count=1000112
    /// e=conflict index at capacity`, votes=0 on a healthy cluster).
    ///
    /// The driver must instead NAME the limit, and do it before the protocol runs
    /// so nothing is registered anywhere and no no-write finalize is owed.
    #[tokio::test]
    async fn an_oversized_write_set_is_refused_by_name_before_the_protocol_runs() {
        // Every replica refuses every RPC: with the guard absent this is a plain
        // quorum failure, so the returned variant discriminates the two paths.
        let transport = flaky(&[(2, usize::MAX), (3, usize::MAX)]);
        let clock = HybridLogicalClock::new(1, 0);
        let write_set = vec![
            (b"k1".to_vec(), b"m1".to_vec()),
            (b"k2".to_vec(), b"m2".to_vec()),
        ];
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            1,
            vec![
                uuid::Uuid::from_u128(1),
                uuid::Uuid::from_u128(2),
                uuid::Uuid::from_u128(3),
            ],
            transport.clone(),
            false,
            &clock,
            write_set,
        )
        .with_conflict_index_capacity(1);

        let result = driver.run_transaction().await;

        match result {
            Err(error @ AccordDriverError::WriteSetExceedsCapacity { keys, capacity }) => {
                assert_eq!(keys, 2, "the write-set has two keys");
                assert_eq!(capacity, 1, "the node was configured for one");
                let rendered = error.to_string();
                assert!(
                    rendered.contains("conflict-index capacity"),
                    "the error must name the limit: {rendered}"
                );
                assert!(
                    rendered.contains("FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY"),
                    "the error must name the knob that fixes it: {rendered}"
                );
                assert!(
                    !rendered.starts_with("abandoned:"),
                    "an oversized write-set is a fault, not a retryable abandon: {rendered}"
                );
            }
            other => panic!("an oversized write-set must be refused by name, not as {other:?}"),
        }

        // The guard fires before the protocol runs: no registration exists on any
        // replica, so no no-write finalize is owed.
        assert!(
            transport.sends.lock().unwrap().is_empty(),
            "the guard must refuse before any RPC is sent"
        );
    }

    /// All protocol phases succeed, but only one RF=3 replica returns an
    /// existence read-vote. Network failures and unexpected replies must not be
    /// promoted into implicit positive votes.
    struct OneReadVoteTransport {
        sole_reader: uuid::Uuid,
    }

    #[async_trait::async_trait]
    impl AccordTransport for OneReadVoteTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{
                PreAcceptOkPayload, PreAcceptPayload, ReadVoteOkPayload, ReadVotePayload,
            };

            match msg {
                Message::AccordPreAccept(bytes) => {
                    let request: PreAcceptPayload = bincode::deserialize(&bytes).unwrap();
                    let response = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: request.t0,
                        deps: Vec::new(),
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                Message::AccordRead(bytes) if host_id == self.sole_reader => {
                    let request: ReadVotePayload = bincode::deserialize(&bytes).unwrap();
                    let response = ReadVoteOkPayload {
                        txn_id: request.txn_id,
                        from: node_id_of(host_id),
                        condition_holds: true,
                        current_row: Vec::new(),
                    };
                    Ok(Message::AccordReadOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                Message::AccordRead(_) => Err(ferrosa_net::error::NetError::Timeout(
                    "read-vote replica unavailable".into(),
                )),
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                other => panic!("unexpected Accord test message: {other:?}"),
            }
        }
    }

    struct ApplyObservedTransport {
        apply_sent: tokio::sync::mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for ApplyObservedTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            assert!(matches!(msg, Message::AccordApply(_)));
            let _ = self.apply_sent.send(());
            Ok(structured_apply_ack(msg, node_id_of(host_id)))
        }
    }

    #[tokio::test]
    async fn apply_fans_out_while_local_dependency_is_parked() {
        let self_id = uuid::Uuid::from_u128(1u128 << 64);
        let remote_id = uuid::Uuid::from_u128(2u128 << 64);
        let (apply_sent, mut apply_received) = tokio::sync::mpsc::unbounded_channel();
        let transport = Arc::new(ApplyObservedTransport { apply_sent });
        let clock = HybridLogicalClock::new(1, 0);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            crate::accord::state_machine::AccordStateMachine::new(
                1,
                Arc::new(ferrosa_storage::accord::sync_writer::MockSyncWriter::new()),
            ),
        ));
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            1,
            vec![self_id, remote_id],
            transport,
            false,
            &clock,
            vec![(b"key".to_vec(), b"mutation".to_vec())],
        )
        .with_local_accord_state(local_state.clone());

        let txn_id = driver.txn_id();
        let dependency = TxnId::new(3, Timestamp::synthetic(1));
        let commit_t = Timestamp::synthetic(2);
        crate::accord::handlers::on_state_machine(&local_state, move |sm| {
            // Register the dependency: a transaction this replica holds no state
            // for can never be applied here, so it would not create a park at all.
            sm.handle_preaccept(dependency, dependency.0, b"key", BallotNumber(0), 0);
            sm.handle_preaccept(txn_id, txn_id.0, b"key", BallotNumber(0), 0);
            sm.handle_commit(txn_id, txn_id.0, commit_t, vec![dependency]);
        })
        .await;

        let apply_task = tokio::spawn(async move {
            driver
                .apply_phase(commit_t, std::collections::HashSet::from([dependency]))
                .await
        });

        // A remote replica must hear the committed txn even while this node is
        // parked on its own dependency. Leave margin before the production
        // dependency-wait deadline so a slow CI host cannot turn this into a
        // timeout-sensitive correctness test.
        let remote_apply =
            tokio::time::timeout(std::time::Duration::from_secs(4), apply_received.recv())
                .await
                .expect("remote Apply must be sent before the local dependency wait expires");
        assert_eq!(remote_apply, Some(()));
        let local_phase = crate::accord::handlers::on_state_machine(&local_state, move |sm| {
            sm.get_state(&txn_id).expect("local txn exists").phase
        })
        .await
        .expect("state machine lock available");
        assert_eq!(local_phase, TxnPhase::Committed);
        assert!(
            !apply_task.is_finished(),
            "must not report success before local Applied"
        );

        crate::accord::handlers::on_state_machine(&local_state, move |sm| {
            sm.handle_apply_writeset(dependency, Vec::new());
        })
        .await;
        assert!(apply_task.await.unwrap().is_ok());
        let local_phase = crate::accord::handlers::on_state_machine(&local_state, move |sm| {
            sm.get_state(&txn_id).expect("local txn exists").phase
        })
        .await
        .expect("state machine lock available");
        assert_eq!(local_phase, TxnPhase::Applied);
    }

    /// A local Apply that cannot resolve its dependencies inside the bound must
    /// ABANDON the transaction — not fail with an opaque network error.
    ///
    /// This branch returned a bare `AccordDriverError::Network(..)`, which the
    /// PostgreSQL front end reports as a generic storage fault (58000). A
    /// transaction that had never been applied was therefore presented to the
    /// client as "something is broken" instead of "not committed, safe to
    /// retry" — and the Jepsen strict-serializability workload, which retries
    /// only on 40001, failed on the 58000 this produced.
    ///
    /// The bound is passed explicitly: `configured_txn_timeout` caches the
    /// environment once per process, so the production bound is out of reach here.
    #[tokio::test]
    async fn a_local_apply_that_never_resolves_abandons_the_transaction() {
        let self_id = uuid::Uuid::from_u128(1u128 << 64);
        let remote_id = uuid::Uuid::from_u128(2u128 << 64);
        let (apply_sent, mut apply_received) = tokio::sync::mpsc::unbounded_channel();
        let transport = Arc::new(ApplyObservedTransport { apply_sent });
        let clock = HybridLogicalClock::new(1, 0);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            crate::accord::state_machine::AccordStateMachine::new(
                1,
                Arc::new(ferrosa_storage::accord::sync_writer::MockSyncWriter::new()),
            ),
        ));
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            1,
            vec![self_id, remote_id],
            transport,
            false,
            &clock,
            vec![(b"key".to_vec(), b"mutation".to_vec())],
        )
        .with_local_accord_state(local_state.clone());

        let txn_id = driver.txn_id();
        let dependency = TxnId::new(3, Timestamp::synthetic(1));
        let commit_t = Timestamp::synthetic(2);
        crate::accord::handlers::on_state_machine(&local_state, move |sm| {
            // Register the dependency: a transaction this replica holds no state
            // for can never be applied here, so it would not create a park at all.
            sm.handle_preaccept(dependency, dependency.0, b"key", BallotNumber(0), 0);
            sm.handle_preaccept(txn_id, txn_id.0, b"key", BallotNumber(0), 0);
            sm.handle_commit(txn_id, txn_id.0, commit_t, vec![dependency]);
        })
        .await;

        // The dependency is never resolved, so the local wait must expire.
        let result = driver
            .apply_phase_within(
                commit_t,
                std::collections::HashSet::from([dependency]),
                std::time::Duration::from_millis(200),
            )
            .await;

        assert_eq!(
            apply_received.try_recv(),
            Ok(()),
            "the local wait must not suppress Apply propagation to the other replicas"
        );
        assert!(
            matches!(result, Err(AccordDriverError::TxnAbandoned { .. })),
            "a local apply that cannot resolve its dependencies must abandon the transaction \
             (retryable 40001), not report an opaque failure; got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Regression: an Apply ack must PROVE it applied THIS transaction.
    //
    // The Apply quorum predicate used to accept an `AccordApplyOK` with an EMPTY
    // body (`b.is_empty() || ...`), so a peer that answered with a bare ApplyOK
    // was counted toward the quorum for WHATEVER txn the coordinator awaited —
    // its `txn_id` was never checked. No production sender emits a bare ApplyOK
    // (`handlers::on_message` always serialises an `ApplyOkPayload`); every
    // empty-body sender lived in this test module. The arm was a test-only
    // affordance that weakened a production safety check the moment any peer was
    // version-skewed (e.g. mid rolling-upgrade) and replied with a bare ApplyOK.
    // -----------------------------------------------------------------------

    /// How a replica answers a single-key `AccordApply` under test.
    #[derive(Clone, Copy)]
    enum ApplyAck {
        /// Pre-fix wire: a bare `AccordApplyOK` carrying no body.
        Empty,
        /// A structured ack for a DIFFERENT transaction than the one awaited.
        MismatchedTxn,
        /// A structured ack echoing the awaited transaction's id.
        MatchingTxn,
    }

    /// A replica that acks every single-key `AccordApply` per `mode`.
    struct ApplyAckTransport {
        mode: ApplyAck,
    }

    #[async_trait::async_trait]
    impl AccordTransport for ApplyAckTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            match msg {
                Message::AccordApply(bytes) => {
                    use crate::accord::wire::{ApplyOkPayload, ApplyPayload};
                    let payload: ApplyPayload = bincode::deserialize(&bytes).unwrap();
                    let txn_id = match self.mode {
                        ApplyAck::Empty => return Ok(Message::AccordApplyOK(Bytes::new())),
                        ApplyAck::MismatchedTxn => TxnId(Timestamp {
                            node: payload.txn_id.0.node.wrapping_add(1),
                            ..payload.txn_id.0
                        }),
                        ApplyAck::MatchingTxn => payload.txn_id,
                    };
                    let ack = ApplyOkPayload {
                        txn_id,
                        from: node_id_of(host_id),
                    };
                    Ok(Message::AccordApplyOK(Bytes::from(
                        bincode::serialize(&ack).unwrap(),
                    )))
                }
                other => panic!("unexpected Apply-phase test message: {other:?}"),
            }
        }
    }

    /// An empty-body `AccordApplyOK` must NOT reach the Apply quorum: it carries
    /// no `txn_id`, so it cannot prove the sender applied THIS transaction. Fails
    /// while the `b.is_empty() ||` arm is present (the bare acks then count) and
    /// passes once the arm is removed.
    #[tokio::test]
    async fn apply_quorum_rejects_empty_body_apply_ok() {
        let replicas = vec![
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        ];
        let transport = Arc::new(ApplyAckTransport {
            mode: ApplyAck::Empty,
        });
        let mut driver = driver_with(transport, replicas);

        let result = driver
            .apply_phase_within(
                make_ts(2000),
                std::collections::HashSet::new(),
                std::time::Duration::from_millis(200),
            )
            .await;

        assert!(
            matches!(result, Err(AccordDriverError::TxnAbandoned { .. })),
            "an empty-body ApplyOK proves nothing about this txn and must not satisfy the \
             Apply quorum (RF=3, coordinator is not a replica → 0 verified acks of the 2 \
             required); got {result:?}"
        );
    }

    /// A structured ack whose `txn_id` is a DIFFERENT transaction than the one
    /// awaited must not count toward the quorum either.
    #[tokio::test]
    async fn apply_quorum_rejects_mismatched_txn_apply_ok() {
        let replicas = vec![
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        ];
        let transport = Arc::new(ApplyAckTransport {
            mode: ApplyAck::MismatchedTxn,
        });
        let mut driver = driver_with(transport, replicas);

        let result = driver
            .apply_phase_within(
                make_ts(2000),
                std::collections::HashSet::new(),
                std::time::Duration::from_millis(200),
            )
            .await;

        assert!(
            matches!(result, Err(AccordDriverError::TxnAbandoned { .. })),
            "an ApplyOK for a different txn_id does not prove THIS txn applied; got {result:?}"
        );
    }

    /// Positive control: a structured ack echoing THIS transaction's id DOES reach
    /// the quorum (2 of 3 replicas acked with the awaited `txn_id`).
    #[tokio::test]
    async fn apply_quorum_accepts_structured_matching_apply_ok() {
        let replicas = vec![
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        ];
        let transport = Arc::new(ApplyAckTransport {
            mode: ApplyAck::MatchingTxn,
        });
        let mut driver = driver_with(transport, replicas);

        let result = driver
            .apply_phase_within(
                make_ts(2000),
                std::collections::HashSet::new(),
                std::time::Duration::from_millis(200),
            )
            .await;

        assert!(
            result.is_ok(),
            "a structured ApplyOK carrying the awaited txn_id must satisfy the Apply quorum; \
             got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Regression: a Commit ack must PROVE it processed THIS transaction.
    //
    // The Commit quorum predicate was `|r| r.is_ok()`, so it accepted ANY `Ok`
    // reply — no variant check, no transaction check. The replica's commit reply
    // was an EMPTY `AccordCommit(Bytes::new())`, so a peer that answered proved
    // nothing about WHICH transaction (if any) it committed, yet counted toward
    // the quorum for whatever txn the coordinator awaited. `handlers::on_message`
    // already had `payload.txn_id` in hand, so echoing it costs nothing. The fix
    // mirrors the Apply ack exactly: a structured `CommitOkPayload { txn_id, from }`
    // that the coordinator deserialises and verifies.
    // -----------------------------------------------------------------------

    /// How a replica answers an `AccordCommit` under test.
    #[derive(Clone, Copy)]
    enum CommitAck {
        /// Pre-fix wire: a bare `AccordCommit` carrying no body.
        Empty,
        /// A structured ack for a DIFFERENT transaction than the one awaited.
        MismatchedTxn,
        /// A structured ack echoing the awaited transaction's id.
        MatchingTxn,
    }

    /// A replica that agrees on the PreAccept (so the coordinator takes the fast
    /// path straight to Commit) and on the Read, acks every Apply structurally,
    /// and answers every Commit per `mode`.
    struct CommitAckTransport {
        mode: CommitAck,
    }

    #[async_trait::async_trait]
    impl AccordTransport for CommitAckTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{
                CommitOkPayload, CommitPayload, PreAcceptOkPayload, PreAcceptPayload,
                ReadVoteOkPayload, ReadVotePayload,
            };
            match msg {
                Message::AccordPreAccept(bytes) => {
                    let request: PreAcceptPayload = bincode::deserialize(&bytes).unwrap();
                    let response = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: request.t0,
                        deps: Vec::new(),
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                Message::AccordRead(bytes) => {
                    let request: ReadVotePayload = bincode::deserialize(&bytes).unwrap();
                    let response = ReadVoteOkPayload {
                        txn_id: request.txn_id,
                        from: node_id_of(host_id),
                        condition_holds: true,
                        current_row: Vec::new(),
                    };
                    Ok(Message::AccordReadOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                Message::AccordCommit(bytes) => {
                    let request: CommitPayload = bincode::deserialize(&bytes).unwrap();
                    let txn_id = match self.mode {
                        CommitAck::Empty => return Ok(Message::AccordCommit(Bytes::new())),
                        CommitAck::MismatchedTxn => TxnId(Timestamp {
                            node: request.txn_id.0.node.wrapping_add(1),
                            ..request.txn_id.0
                        }),
                        CommitAck::MatchingTxn => request.txn_id,
                    };
                    let ack = CommitOkPayload {
                        txn_id,
                        from: node_id_of(host_id),
                    };
                    Ok(Message::AccordCommit(Bytes::from(
                        bincode::serialize(&ack).unwrap(),
                    )))
                }
                other => panic!("unexpected commit-phase test message: {other:?}"),
            }
        }
    }

    /// An RF=3 driver whose coordinator is itself a replica (1 implicit self ack
    /// of the 2 required) and whose two remotes answer Commit per `mode`.
    fn commit_ack_driver(mode: CommitAck) -> AccordCoordinatorDriver {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);

        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));
        let transport = Arc::new(CommitAckTransport { mode });
        let clock = HybridLogicalClock::new(self_node, 0);
        AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport,
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
    }

    /// An empty-body `AccordCommit` must NOT reach the commit quorum: it carries
    /// no `txn_id`, so it cannot prove the sender processed THIS transaction. Fails
    /// while the predicate is `|r| r.is_ok()` (the bare acks then count) and passes
    /// once the predicate verifies the payload's `txn_id`.
    #[tokio::test]
    async fn commit_quorum_rejects_empty_body_commit_ack() {
        let mut driver = commit_ack_driver(CommitAck::Empty);

        let result = driver.run_transaction().await;

        assert!(
            matches!(result, Err(AccordDriverError::QuorumUnavailable)),
            "an empty-body Commit ack proves nothing about this txn and must not satisfy the \
             commit quorum (RF=3, coordinator is a replica → 1 implicit self ack of the 2 \
             required); got {result:?}"
        );
    }

    /// A structured ack whose `txn_id` is a DIFFERENT transaction than the one
    /// awaited must not count toward the commit quorum either.
    #[tokio::test]
    async fn commit_quorum_rejects_mismatched_txn_commit_ack() {
        let mut driver = commit_ack_driver(CommitAck::MismatchedTxn);

        let result = driver.run_transaction().await;

        assert!(
            matches!(result, Err(AccordDriverError::QuorumUnavailable)),
            "a Commit ack for a different txn_id does not prove THIS txn committed; got {result:?}"
        );
    }

    /// Positive control: a structured ack echoing THIS transaction's id DOES reach
    /// the commit quorum (the coordinator's implicit self ack + one matching remote
    /// = 2 of 3).
    #[tokio::test]
    async fn commit_quorum_accepts_structured_matching_commit_ack() {
        let mut driver = commit_ack_driver(CommitAck::MatchingTxn);

        let result = driver.run_transaction().await;

        assert!(
            result.is_ok(),
            "a structured Commit ack carrying the awaited txn_id must satisfy the commit quorum; \
             got {result:?}"
        );
    }

    #[tokio::test]
    async fn one_true_existence_vote_plus_two_failures_does_not_apply_rf3() {
        let replicas = vec![
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            uuid::Uuid::from_u128(3),
        ];
        let transport = Arc::new(OneReadVoteTransport {
            sole_reader: replicas[0],
        });
        let mut driver = driver_with(transport, replicas);

        let result = driver.run_transaction().await;

        assert!(
            matches!(result, Err(AccordDriverError::QuorumUnavailable)),
            "one explicit true vote is below F+1 at RF=3; got {result:?}"
        );
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
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_read_predicate(crate::accord::wire::ReadPredicate::Always);

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
    ///   local vote plus that remote vote prove a fast quorum is impossible and
    ///   the coordinator must move to Accept without waiting for `remote1`.
    /// - Accept: `remote1` votes; `remote2` never replies. Slow quorum (RF=3) is
    ///   2, so the local Accept vote plus `remote1` must complete the phase.
    ///   Waiting for every remote response would make this transaction hang.
    /// - Commit/Apply: every remote acks, so only the Accept phase is stressed.
    struct SlowPathSelfVoteTransport {
        remote1: uuid::Uuid,
        remote2: uuid::Uuid,
        t0: Timestamp,
        conflict_t: Timestamp,
        accept_targets: parking_lot::Mutex<Vec<uuid::Uuid>>,
        accept_delay_hits: std::sync::atomic::AtomicUsize,
    }

    fn node_id_of(host: uuid::Uuid) -> u64 {
        u64::from_be_bytes(host.as_bytes()[..8].try_into().unwrap())
    }

    /// Decode the `txn_id` from an inbound Apply request — the v1 single-key
    /// `AccordApply` or the multi-key `AccordApplyV2` — and serialise the
    /// structured `AccordApplyOK` a production replica replies with. Every test
    /// double that answers an Apply MUST use this: an empty-body ack carries no
    /// `txn_id` and is NOT counted toward the Apply quorum (it cannot prove which
    /// transaction, if any, the peer applied).
    fn structured_apply_ack(msg: Message, from: u64) -> Message {
        use crate::accord::wire::{ApplyOkPayload, ApplyPayload, ApplyV2Payload};
        let txn_id = match msg {
            Message::AccordApply(b) => {
                bincode::deserialize::<ApplyPayload>(&b)
                    .expect("v1 Apply payload decodes")
                    .txn_id
            }
            Message::AccordApplyV2(b) => {
                bincode::deserialize::<ApplyV2Payload>(&b)
                    .expect("v2 Apply payload decodes")
                    .txn_id
            }
            other => panic!("expected an Apply request, got {other:?}"),
        };
        let ack = ApplyOkPayload { txn_id, from };
        Message::AccordApplyOK(Bytes::from(bincode::serialize(&ack).unwrap()))
    }

    /// Decode the `txn_id` from an inbound `AccordCommit` request and serialise
    /// the structured `AccordCommit` a production replica replies with. Every
    /// test double that answers a Commit MUST use this: an empty-body reply
    /// carries no `txn_id` and is NOT counted toward the commit quorum (it cannot
    /// prove which transaction, if any, the peer committed).
    fn structured_commit_ack(msg: Message, from: u64) -> Message {
        use crate::accord::wire::{CommitOkPayload, CommitPayload};
        let txn_id = match msg {
            Message::AccordCommit(b) => {
                bincode::deserialize::<CommitPayload>(&b)
                    .expect("Commit payload decodes")
                    .txn_id
            }
            other => panic!("expected a Commit request, got {other:?}"),
        };
        let ack = CommitOkPayload { txn_id, from };
        Message::AccordCommit(Bytes::from(bincode::serialize(&ack).unwrap()))
    }

    #[async_trait::async_trait]
    impl AccordTransport for SlowPathSelfVoteTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{
                AcceptOkPayload, AcceptPayload, PreAcceptOkPayload, ReadVoteOkPayload,
                ReadVotePayload,
            };
            match msg {
                Message::AccordPreAccept(_) | Message::AccordPreAcceptV2(_) => {
                    if host_id == self.remote1 {
                        std::future::pending::<()>().await;
                        unreachable!("the unanswered PreAccept replica must stay pending");
                    }
                    let (t, deps) = if host_id == self.remote1 {
                        (self.t0, vec![])
                    } else if host_id == self.remote2 {
                        (self.conflict_t, Vec::new())
                    } else {
                        panic!("PreAccept to an unexpected host {host_id} (self must be local)");
                    };
                    let payload = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t,
                        deps,
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                Message::AccordAccept(b) => {
                    self.accept_targets.lock().push(host_id);
                    if host_id == self.remote1 {
                        self.accept_delay_hits
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let ap: AcceptPayload = bincode::deserialize(&b).unwrap();
                        let ok = AcceptOkPayload {
                            txn_id: ap.txn_id,
                            deps: ap.deps,
                        };
                        Ok(Message::AccordAcceptOK(Bytes::from(
                            bincode::serialize(&ok).unwrap(),
                        )))
                    } else if host_id == self.remote2 {
                        // This replica stays silent in both phases. Its PreAccept
                        // must not block the proven NeedAccept decision; its Accept
                        // must not block the local + remote slow quorum.
                        std::future::pending::<()>().await;
                        unreachable!("the unanswered Accept replica must stay pending");
                    } else {
                        // A self-send would land here under the pre-fix code — the
                        // recorded target is what the test's assertion catches.
                        Err(ferrosa_net::error::NetError::Timeout(
                            "unexpected Accept target (self must be local)".into(),
                        ))
                    }
                }
                Message::AccordRead(bytes) => {
                    let read: ReadVotePayload = bincode::deserialize(&bytes).unwrap();
                    let payload = ReadVoteOkPayload {
                        txn_id: read.txn_id,
                        from: node_id_of(host_id),
                        condition_holds: true,
                        current_row: Vec::new(),
                    };
                    Ok(Message::AccordReadOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                // Anything else: ack so only Accept is stressed.
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                _ => Ok(Message::AccordCommit(Bytes::new())),
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

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
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
            accept_targets: parking_lot::Mutex::new(Vec::new()),
            accept_delay_hits: std::sync::atomic::AtomicUsize::new(0),
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

        let completed =
            tokio::time::timeout(std::time::Duration::from_secs(1), driver.run_transaction()).await;
        assert!(
            completed.is_ok(),
            "PreAccept and Accept must complete without their silent minority replies; \
             Accept targets before timeout: {:?}",
            transport.accept_targets.lock()
        );
        let result = completed.expect("completion was checked above");
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
        assert_eq!(
            transport
                .accept_delay_hits
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the responsive remote Accept reply must be processed exactly once"
        );
    }

    #[tokio::test]
    async fn preaccept_deadline_uses_slow_quorum_without_waiting_for_silent_peer() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));
        let t0 = make_ts(1000);
        let transport = Arc::new(SlowPathSelfVoteTransport {
            remote1,
            remote2,
            t0,
            conflict_t: t0,
            accept_targets: parking_lot::Mutex::new(Vec::new()),
            accept_delay_hits: std::sync::atomic::AtomicUsize::new(0),
        });
        let clock = HybridLogicalClock::new(self_node, 0);
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport.clone(),
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            driver.run_transaction(),
        )
        .await;
        assert!(
            result.is_ok(),
            "a valid slow PreAccept quorum must enter Accept after the configured \
             fast-path window instead of waiting for the silent replica"
        );
        assert!(
            result.unwrap().is_ok(),
            "the local vote plus one remote vote still must satisfy the RF=3 slow quorum"
        );
        assert_eq!(
            transport
                .accept_delay_hits
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the slow path must send Accept while the other PreAccept remains pending"
        );
    }

    #[test]
    fn preaccept_fast_path_timeout_defaults_and_rejects_bad_values() {
        assert_eq!(
            parse_preaccept_fast_path_timeout(None),
            Ok(std::time::Duration::from_millis(
                DEFAULT_PREACCEPT_FAST_PATH_TIMEOUT_MS
            ))
        );
        assert_eq!(
            parse_preaccept_fast_path_timeout(Some("25")),
            Ok(std::time::Duration::from_millis(25))
        );
        assert!(parse_preaccept_fast_path_timeout(Some("0")).is_err());
        assert!(parse_preaccept_fast_path_timeout(Some("fast")).is_err());
    }

    #[tokio::test]
    async fn preaccept_timeout_does_not_count_as_a_slow_quorum_vote() {
        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);
        let t0 = make_ts(1000);
        let transport = Arc::new(SlowPathSelfVoteTransport {
            remote1,
            remote2,
            t0,
            conflict_t: t0,
            accept_targets: parking_lot::Mutex::new(Vec::new()),
            accept_delay_hits: std::sync::atomic::AtomicUsize::new(0),
        });
        let clock = HybridLogicalClock::new(self_node, 0);
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport.clone(),
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            driver.run_transaction(),
        )
        .await;
        assert!(result.is_err(), "one remote vote is below RF=3 slow quorum");
        assert!(
            transport.accept_targets.lock().is_empty(),
            "an elapsed timer must never cause Accept without an actual slow quorum"
        );
    }

    #[tokio::test]
    async fn snapshot_preaccept_timeout_requires_marker_key_quorum() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));
        let t0 = make_ts(1000);
        let transport = Arc::new(SlowPathSelfVoteTransport {
            remote1,
            remote2,
            t0,
            conflict_t: t0,
            accept_targets: parking_lot::Mutex::new(Vec::new()),
            accept_delay_hits: std::sync::atomic::AtomicUsize::new(0),
        });
        let clock = HybridLogicalClock::new(self_node, 0);
        let marker_key = ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_MARKER_KEY;
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport.clone(),
            false,
            &clock,
            vec![
                (b"k".to_vec(), b"m".to_vec()),
                (marker_key.to_vec(), b"marker".to_vec()),
            ],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10))
        .with_postgres_snapshot(make_ts(500))
        .with_per_key_replicas(Arc::new(move |key| {
            if key == marker_key {
                vec![self_host, remote2]
            } else {
                vec![self_host, remote1, remote2]
            }
        }));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(250),
            driver.run_transaction(),
        )
        .await;
        assert!(
            result.is_ok() && result.unwrap().is_ok(),
            "marker replicas supplied a slow quorum, so the silent data-only \
             replica must not block the slow path"
        );
    }

    #[tokio::test]
    async fn snapshot_preaccept_timeout_waits_when_marker_quorum_is_missing() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));
        let t0 = make_ts(1000);
        let transport = Arc::new(SlowPathSelfVoteTransport {
            remote1,
            remote2,
            t0,
            conflict_t: t0,
            accept_targets: parking_lot::Mutex::new(Vec::new()),
            accept_delay_hits: std::sync::atomic::AtomicUsize::new(0),
        });
        let clock = HybridLogicalClock::new(self_node, 0);
        let marker_key = ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_MARKER_KEY;
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport.clone(),
            false,
            &clock,
            vec![
                (b"k".to_vec(), b"m".to_vec()),
                (marker_key.to_vec(), b"marker".to_vec()),
            ],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10))
        .with_postgres_snapshot(make_ts(500))
        .with_per_key_replicas(Arc::new(move |key| {
            if key == marker_key {
                vec![self_host, remote1]
            } else {
                vec![self_host, remote1, remote2]
            }
        }));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            driver.run_transaction(),
        )
        .await;
        assert!(
            result.is_err(),
            "the marker slow quorum is still incomplete"
        );
        assert!(
            transport.accept_targets.lock().is_empty(),
            "the transaction-wide slow quorum cannot stand in for the marker-key quorum"
        );
    }

    struct LateStaleMarkerTransport {
        remote1: uuid::Uuid,
        accept_targets: parking_lot::Mutex<Vec<uuid::Uuid>>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for LateStaleMarkerTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{
                AcceptOkPayload, AcceptPayload, PreAcceptOkPayload, PreAcceptV2Payload,
            };
            match msg {
                Message::AccordPreAcceptV2(bytes) => {
                    let payload: PreAcceptV2Payload = bincode::deserialize(&bytes).unwrap();
                    if host_id == self.remote1 {
                        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                    }
                    let response = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: payload.t0,
                        deps: Vec::new(),
                        snapshot_stale: host_id == self.remote1,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                Message::AccordAccept(bytes) => {
                    self.accept_targets.lock().push(host_id);
                    let payload: AcceptPayload = bincode::deserialize(&bytes).unwrap();
                    Ok(Message::AccordAcceptOK(Bytes::from(
                        bincode::serialize(&AcceptOkPayload {
                            txn_id: payload.txn_id,
                            deps: payload.deps,
                        })
                        .unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                _ => Ok(Message::AccordCommit(Bytes::new())),
            }
        }
    }

    #[tokio::test]
    async fn late_snapshot_stale_from_marker_replica_still_aborts_before_accept() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
        let self_node = node_id_of(self_host);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(self_node, Arc::new(MockSyncWriter::new())),
        ));
        let transport = Arc::new(LateStaleMarkerTransport {
            remote1,
            accept_targets: parking_lot::Mutex::new(Vec::new()),
        });
        let clock = HybridLogicalClock::new(self_node, 0);
        let marker_key = ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_MARKER_KEY;
        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            self_node,
            vec![self_host, remote1, remote2],
            transport.clone(),
            false,
            &clock,
            vec![
                (b"k".to_vec(), b"m".to_vec()),
                (marker_key.to_vec(), b"marker".to_vec()),
            ],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10))
        .with_postgres_snapshot(make_ts(500))
        .with_per_key_replicas(Arc::new(move |key| {
            if key == marker_key {
                vec![remote1, remote2]
            } else {
                vec![self_host, remote1, remote2]
            }
        }));

        let result = driver.run_transaction().await;
        assert!(
            matches!(result, Err(AccordDriverError::SnapshotStale)),
            "the marker replica's late stale vote must veto this snapshot: {result:?}"
        );
        assert!(
            transport.accept_targets.lock().is_empty(),
            "the driver must keep collecting marker votes after the deadline"
        );
    }

    /// `stale_peer` vetoes the PostgreSQL snapshot at once; `slow_peer` answers
    /// PreAcceptOK only after `slow_delay` (a paused node). Records when each
    /// replica's PreAccept answered and when each received an Apply.
    struct PausedPeerTransport {
        stale_peer: uuid::Uuid,
        slow_peer: uuid::Uuid,
        slow_delay: std::time::Duration,
        events: parking_lot::Mutex<Vec<(&'static str, uuid::Uuid, std::time::Instant)>>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for PausedPeerTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{PreAcceptOkPayload, PreAcceptV2Payload};
            match msg {
                Message::AccordPreAcceptV2(bytes) => {
                    let payload: PreAcceptV2Payload = bincode::deserialize(&bytes).unwrap();
                    if host_id == self.slow_peer {
                        tokio::time::sleep(self.slow_delay).await;
                    }
                    self.events
                        .lock()
                        .push(("preaccept_ok", host_id, std::time::Instant::now()));
                    let response = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: payload.t0,
                        deps: Vec::new(),
                        snapshot_stale: host_id == self.stale_peer,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&response).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    self.events
                        .lock()
                        .push(("apply", host_id, std::time::Instant::now()));
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                _ => Ok(Message::AccordCommit(Bytes::new())),
            }
        }
    }

    fn paused_peer_driver(
        transport: Arc<PausedPeerTransport>,
        local_state: crate::accord::handlers::AccordState,
        self_host: uuid::Uuid,
    ) -> AccordCoordinatorDriver {
        let clock = HybridLogicalClock::new(node_id_of(self_host), 0);
        let marker_key = ferrosa_storage::accord::conflict_index::POSTGRES_TRANSACTION_MARKER_KEY;
        AccordCoordinatorDriver::new_multi_with_transport(
            node_id_of(self_host),
            vec![self_host, transport.stale_peer, transport.slow_peer],
            transport.clone(),
            false,
            &clock,
            vec![
                (b"k".to_vec(), b"m".to_vec()),
                (marker_key.to_vec(), b"marker".to_vec()),
            ],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_preaccept_fast_path_timeout(std::time::Duration::from_millis(10))
        .with_postgres_snapshot(make_ts(500))
    }

    /// The PostgreSQL Jepsen fault schedule failure (2026-09-29).
    ///
    /// With node 3 paused, a transaction whose snapshot one live replica had
    /// already rejected as stale was doomed at once, but its coordinator kept
    /// draining PreAccept responses until node 3's RPC timed out 10 s later.
    /// All that time the dead transaction stayed registered as a conflict, so
    /// every PostgreSQL snapshot barrier on its keys dep-waited 5 s and failed
    /// "dependencies were not applied locally". A doomed transaction must fail
    /// and release its keys at once, not wait on a paused replica.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_doomed_transaction_releases_its_keys_without_waiting_on_a_paused_replica() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let transport = Arc::new(PausedPeerTransport {
            stale_peer: uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111),
            slow_peer: uuid::Uuid::from_u128((0x3333_u128 << 64) | 0x3333),
            slow_delay: std::time::Duration::from_secs(3),
            events: parking_lot::Mutex::new(Vec::new()),
        });
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(node_id_of(self_host), Arc::new(MockSyncWriter::new())),
        ));
        let mut driver = paused_peer_driver(transport.clone(), local_state.clone(), self_host);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(1500),
            driver.run_transaction(),
        )
        .await
        .expect("a doomed transaction must not wait for the paused replica's RPC to time out");

        assert!(
            matches!(result, Err(AccordDriverError::SnapshotStale)),
            "{result:?}"
        );
        assert!(
            local_state
                .lock()
                .unapplied_conflicts_before(b"k", &make_ts(u64::MAX / 2))
                .is_empty(),
            "the failed transaction must no longer block conflicting barriers here"
        );
    }

    /// The guard the old drain provided, kept after returning early: a slow
    /// replica that registers the doomed transaction AFTER the coordinator
    /// gave up still receives a no-write finalize, so the late registration
    /// cannot poison its keys (FMEA CL-20).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_late_preaccept_registration_is_finalized_after_an_early_abort() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let slow_peer = uuid::Uuid::from_u128((0x3333_u128 << 64) | 0x3333);
        let transport = Arc::new(PausedPeerTransport {
            stale_peer: uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111),
            slow_peer,
            slow_delay: std::time::Duration::from_millis(300),
            events: parking_lot::Mutex::new(Vec::new()),
        });
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(node_id_of(self_host), Arc::new(MockSyncWriter::new())),
        ));
        let mut driver = paused_peer_driver(transport.clone(), local_state, self_host);

        let result = driver.run_transaction().await;
        assert!(
            matches!(result, Err(AccordDriverError::SnapshotStale)),
            "{result:?}"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let finalized_after_late_registration = loop {
            let events = transport.events.lock().clone();
            let late_ok = events
                .iter()
                .find(|(kind, peer, _)| *kind == "preaccept_ok" && *peer == slow_peer)
                .map(|(_, _, at)| *at);
            let found = late_ok.is_some_and(|registered| {
                events.iter().any(|(kind, peer, at)| {
                    *kind == "apply" && *peer == slow_peer && *at >= registered
                })
            });
            if found || std::time::Instant::now() >= deadline {
                break found;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        };
        assert!(
            finalized_after_late_registration,
            "the slow replica registered the txn late and must be finalized after that"
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
            use crate::accord::wire::{
                PreAcceptOkPayload, PreAcceptPayload, ReadVoteOkPayload, ReadVotePayload,
            };
            match msg {
                Message::AccordPreAccept(b) => {
                    self.preaccept_targets.lock().push(host_id);
                    // Echo the coordinator's own t0 so this replica genuinely agrees.
                    let pa: PreAcceptPayload = bincode::deserialize(&b).unwrap();
                    let payload = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: pa.t0,
                        deps: vec![],
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                Message::AccordRead(bytes) => {
                    let read: ReadVotePayload = bincode::deserialize(&bytes).unwrap();
                    let payload = ReadVoteOkPayload {
                        txn_id: read.txn_id,
                        from: node_id_of(host_id),
                        condition_holds: true,
                        current_row: Vec::new(),
                    };
                    Ok(Message::AccordReadOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                // Anything else: ack.
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                _ => Ok(Message::AccordCommit(Bytes::new())),
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

        let self_host = uuid::Uuid::from_u128((0xC0DE_u128 << 64) | 0xC0DE);
        let remote1 = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let remote2 = uuid::Uuid::from_u128((0x2222_u128 << 64) | 0x2222);
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

    /// A replica that holds `row` for every key: answers every read-vote with
    /// those row bytes, and with `condition_holds = row.is_empty()` for the
    /// existence predicate (the row is absent only when `row` is empty).
    struct RowHoldingReplicaTransport {
        row: Vec<u8>,
    }

    #[async_trait::async_trait]
    impl AccordTransport for RowHoldingReplicaTransport {
        async fn send(
            &self,
            host_id: uuid::Uuid,
            msg: Message,
            _lane: ferrosa_net::codec::Lane,
        ) -> ferrosa_net::error::Result<Message> {
            use crate::accord::wire::{
                PreAcceptOkPayload, PreAcceptPayload, ReadVoteOkPayload, ReadVotePayload,
            };
            match msg {
                Message::AccordPreAccept(b) => {
                    let pa: PreAcceptPayload = bincode::deserialize(&b).unwrap();
                    let payload = PreAcceptOkPayload {
                        from: node_id_of(host_id),
                        t: pa.t0,
                        deps: vec![],
                        snapshot_stale: false,
                    };
                    Ok(Message::AccordPreAcceptOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                Message::AccordRead(bytes) => {
                    let read: ReadVotePayload = bincode::deserialize(&bytes).unwrap();
                    let payload = ReadVoteOkPayload {
                        txn_id: read.txn_id,
                        from: node_id_of(host_id),
                        condition_holds: self.row.is_empty(),
                        current_row: self.row.clone(),
                    };
                    Ok(Message::AccordReadOK(Bytes::from(
                        bincode::serialize(&payload).unwrap(),
                    )))
                }
                apply @ (Message::AccordApply(_) | Message::AccordApplyV2(_)) => {
                    Ok(structured_apply_ack(apply, node_id_of(host_id)))
                }
                commit @ Message::AccordCommit(_) => {
                    Ok(structured_commit_ack(commit, node_id_of(host_id)))
                }
                _ => Ok(Message::AccordCommit(Bytes::new())),
            }
        }
    }

    /// The coordinator's own engine does not hold the key: its local
    /// read-at-`t` finds nothing.
    struct AbsentLocalReader;

    impl crate::accord::apply::StorageReader for AbsentLocalReader {
        fn read_row_at(
            &self,
            _keyspace: &str,
            _table: &str,
            _key: &[u8],
            _t: Timestamp,
        ) -> Result<Option<Vec<u8>>, crate::accord::apply::RowReadError> {
            Ok(None)
        }
    }

    /// t_0bcd56f7: the coordinator is NOT a replica of the key (RF=1 on a
    /// 3-node cluster, the key owned by another node). The one replica holds
    /// the row, so a conditional UPDATE whose condition the row satisfies must
    /// apply. The coordinator's own engine has no copy of the key, and its empty
    /// local read must not be counted as a replica's vote -- at RF=1 it alone
    /// was the F+1 "agreement" that the row is absent.
    #[tokio::test]
    async fn a_non_replica_coordinator_does_not_vote_on_a_generic_if() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        // Distinct in both halves: node ids come from the high 64 bits.
        let coordinator_host = uuid::Uuid::from_u128((0x3333_u128 << 64) | 0x3333);
        let replica = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let coordinator_node = node_id_of(coordinator_host);
        let clock = HybridLogicalClock::new(coordinator_node, 0);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(coordinator_node, Arc::new(MockSyncWriter::new())),
        ));

        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            coordinator_node,
            vec![replica], // RF=1, and the coordinator is not the replica
            Arc::new(RowHoldingReplicaTransport {
                row: b"row-with-phase-offered".to_vec(),
            }),
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_reader(Arc::new(AbsentLocalReader))
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()))
        .with_read_predicate(crate::accord::wire::ReadPredicate::ReadRow {
            keyspace: "ks".into(),
            table: "t".into(),
        })
        .with_condition_gate(Box::new(|row| row.is_some_and(|r| !r.is_empty())));

        let result = driver.run_transaction().await;
        assert!(
            result.is_ok(),
            "the only replica holds the row, so the condition holds: {result:?}"
        );
        assert_eq!(driver.last_read_row(), Some(&b"row-with-phase-offered"[..]));
    }

    /// The same for `INSERT IF NOT EXISTS`: a non-replica coordinator's local
    /// "absent" must not outvote the replica that holds the row.
    #[tokio::test]
    async fn a_non_replica_coordinator_does_not_vote_on_if_not_exists() {
        use crate::accord::state_machine::AccordStateMachine;
        use ferrosa_storage::accord::sync_writer::MockSyncWriter;

        // Distinct in both halves: node ids come from the high 64 bits.
        let coordinator_host = uuid::Uuid::from_u128((0x3333_u128 << 64) | 0x3333);
        let replica = uuid::Uuid::from_u128((0x1111_u128 << 64) | 0x1111);
        let coordinator_node = node_id_of(coordinator_host);
        let clock = HybridLogicalClock::new(coordinator_node, 0);
        let local_state: crate::accord::handlers::AccordState = Arc::new(parking_lot::Mutex::new(
            AccordStateMachine::new(coordinator_node, Arc::new(MockSyncWriter::new())),
        ));

        let mut driver = AccordCoordinatorDriver::new_multi_with_transport(
            coordinator_node,
            vec![replica],
            Arc::new(RowHoldingReplicaTransport {
                row: b"existing-row".to_vec(),
            }),
            false,
            &clock,
            vec![(b"k".to_vec(), b"m".to_vec())],
        )
        .with_local_accord_state(local_state)
        .with_local_applier(Arc::new(crate::accord::apply::NoopStorageApplier::new()));

        let result = driver.run_transaction().await;
        assert!(
            matches!(result, Err(AccordDriverError::ConditionNotMet { .. })),
            "the replica holds the row, so IF NOT EXISTS must not apply: {result:?}"
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
            let reply = match &msg {
                Message::AccordApply(_) | Message::AccordApplyV2(_) => {
                    structured_apply_ack(msg.clone(), node_id_of(host_id))
                }
                Message::AccordCommit(_) => structured_commit_ack(msg.clone(), node_id_of(host_id)),
                _ => Message::AccordCommit(Bytes::new()),
            };
            self.sent.lock().insert(host_id, msg);
            Ok(reply)
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
            slow: std::collections::HashSet::new(),
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
            slow: std::collections::HashSet::new(),
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

    #[tokio::test]
    async fn quorum_broadcast_returns_without_waiting_for_slow_minority_peers() {
        let n = six_nodes();
        let behavior = [(n[0], true), (n[1], true), (n[3], true), (n[4], true)]
            .into_iter()
            .collect();
        let slow = [n[2], n[5]].into_iter().collect();
        let mock = Arc::new(MockTransport {
            behavior,
            slow,
            ok: Message::AccordApplyOK(Bytes::new()),
        });
        let driver = driver_with(mock, n.clone());

        let reached = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            driver.quorum_broadcast(Message::AccordCommit(Bytes::new()), &two_shards(&n), |r| {
                r.is_ok()
            }),
        )
        .await
        .expect("quorum must not wait for unanswered minority replicas");

        assert!(reached, "both shards have enough responsive replicas");
    }
}
