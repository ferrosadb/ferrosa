---
type: proposal
status: in-progress
priority: P0
created: 2026-05-09
sprint: 3
companion-to: sprint-03-openraft-fork.md, ../decisions/012-prevote-checkquorum-leadership-transfer.md
---

# Sprint 3 — openraft Fork Patches: Implementation Status and Remaining Pseudo-Code

This document tracks what landed in the `correctness/prevote-checkquorum`
branch of `/home/bkearns/src/ferrosa-openraft` (a local clone of
`databendlabs/openraft` that an operator will later push to
`ferrosadb/openraft`), and what remains as pseudo-code for the deeper
engine state-machine work.

## Local fork state

- Repo: `/home/bkearns/src/ferrosa-openraft`
- Branch: `correctness/prevote-checkquorum` (off `ferrosadb/fix/separate-replication-timeout`).
- Upstream remote: `databendlabs/openraft`. Ferrosadb remote: `ferrosadb/openraft`.
- Build: `cargo build -p openraft` clean on stable Rust (the existing
  `rust-toolchain` pin to nightly-2025-01-01 was removed because validit
  now requires `let-chains` which is unstable on that toolchain). CI must
  use stable.

## Per-work-item status

| WI    | Status | Commit (local) | Notes |
|-------|--------|---------------|-------|
| W3.1  | Partial — types & trait surface | `1048f911` | Message types + RaftNetwork default impls + RPCTypes variants. Engine handler `handle_pre_vote_req` deferred (see "Pseudo-code" below). |
| W3.1 REFACTOR | Done | `<refactor commit>` | `is_log_up_to_date` extracted as pure function. |
| W3.2  | Partial — predicate landed | `<refactor commit>` | `LeaderLease` + `evaluate_pre_vote` cover the lease-aware rejection at the decision-function level. Engine wire-up deferred. |
| W3.3  | NOT STARTED — pseudo-code only | — | Requires new `ServerState::PreCandidate` and election state machine refactor. Substantial. See below. |
| W3.4  | Partial — pure-function repro lands | `<refactor commit>` | Test `w3_4_runaway_term_repro_partitioned_candidate_with_stale_log` proves the protocol fix at the decision level. The full multi-node integration test follows once W3.3 lands. |
| W3.5  | Done — decision + tick handler | `58365ff3` | `CheckQuorum` decision struct + `handle_tick_check_quorum` calling `engine.leader_step_down()`. |
| W3.5 REFACTOR | Partial | `<refactor commit>` | `LeaderLease` type extracted but not yet stored on `LeaderData`. |
| W3.6  | Done by construction | `58365ff3` | `Healthy` decision is returned whenever `elapsed < deadline`; no test regression. Multi-node integration test deferred. |
| W3.7  | Partial — predicate covers it | `<refactor commit>` | `pre_vote_granted_after_lease_invalidation_w3_7` test proves the contract. Engine wire-up between `leader_step_down` and `LeaderLease::invalidate` deferred. |
| W3.8  | Partial — types & trait surface | `1048f911` | `TimeoutNowRequest`/`Response` + trait method default. Engine handler deferred. |
| W3.9  | NOT STARTED — pseudo-code only | — | `Raft::trigger().transfer_to(node_id)` async API + drain logic. See below. |
| W3.10 | NOT STARTED — pseudo-code only | — | Timeout safety in transfer_to. Trivially testable once W3.9 lands. |
| W3.11 | Done | (ferrosa side) | Repointed Cargo.toml to local path. |
| W3.12 | Partial — wired but gap-test fails | (ferrosa side) | Knobs exposed; runaway-term repro **still fails** because the engine-side PreVote handler is not wired. This is the documented gap that the engine-side patches close. |
| W3.13 | NOT STARTED — pseudo-code only | — | Depends on W3.9. |

`<refactor commit>` is the second commit in the local branch
(`1048f911` → CheckQuorum → predicate-modules → ferrosa integration).

## Pseudo-code for the deferred work

### W3.3 — `ServerState::PreCandidate` and election state machine

```rust
// openraft/src/core/server_state.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum ServerState {
    Learner,
    Follower,
    PreCandidate,   // NEW (W3.3, ADR-012). Probing peers with PreVoteRequest.
    Candidate,
    Leader,
}

// openraft/src/raft_state/io_state/server_state.rs
//   `calc_server_state(...)` extended:
//     - When `vote.is_committed() == false` AND `enable_pre_vote == true`
//       AND we have just timed out the leader, transition Follower → PreCandidate
//       (NOT Candidate). Vote::term is NOT yet incremented; we use a *prospective*
//       vote for the pre-vote round.
//     - On pre-vote majority ack: PreCandidate → Candidate (NOW increment term,
//       start real election).
//     - On pre-vote rejection: PreCandidate → Follower (revert prospective vote;
//       persistent vote unchanged).

// openraft/src/proposer/candidate.rs (new file, parallel to existing candidate.rs)
pub(crate) struct PreCandidateState<C: RaftTypeConfig> {
    /// Prospective vote — `Vote::new_non_committed(self.current_term + 1, self.id)`.
    /// Held in memory only; never written to storage.
    prospective_vote: Vote<C::NodeId>,
    /// Pre-grants seen so far. The candidate only transitions to real Candidate
    /// once a quorum (`Membership::quorum_for_vote`) of distinct voters pre-grants.
    pre_grants: BTreeSet<C::NodeId>,
    pre_rejects: BTreeSet<C::NodeId>,
}

impl<C: RaftTypeConfig> PreCandidateState<C> {
    pub fn record_pre_grant(&mut self, voter: C::NodeId) -> PreVoteResult {
        self.pre_grants.insert(voter);
        if self.has_quorum() {
            PreVoteResult::Promote
        } else {
            PreVoteResult::Continue
        }
    }
    pub fn record_pre_reject(&mut self, voter: C::NodeId, voter_term: u64) -> PreVoteResult {
        self.pre_rejects.insert(voter);
        if voter_term > self.prospective_vote.leader_id().get_term() {
            // Pre-vote saw a higher term. Revert to follower; do NOT advance our term.
            // The follower's higher term will be observed via AppendEntries in due course.
            PreVoteResult::RevertToFollower
        } else if self.would_exceed_minority(...) {
            PreVoteResult::RevertToFollower
        } else {
            PreVoteResult::Continue
        }
    }
}

pub(crate) enum PreVoteResult {
    Continue,
    Promote,            // → Candidate (now increment term, start real election)
    RevertToFollower,   // → Follower (no term advance — the W3.4 fix)
}

// openraft/src/engine/handler/following_handler/handle_pre_vote_req.rs (NEW)
//   Mirrors handle_vote_req but does NOT mutate Vote state and consults LeaderLease.
//
//   pub fn handle_pre_vote_req(&mut self, req: PreVoteRequest<C::NodeId>) -> PreVoteResponse<C::NodeId> {
//       let now = C::now();
//       let elapsed_since_last_leader_msg = self.state.last_heard_from_leader().map(|t| now - t);
//       let lease = LeaderLease {
//           election_timeout_ms: self.config.timer_config.election_timeout.as_millis() as u64,
//           invalidated: self.state.lease_invalidated,
//       };
//       let lease_status = lease.is_active(elapsed_since_last_leader_msg);
//
//       let decision = evaluate_pre_vote(
//           req.vote.leader_id().get_term(),
//           self.state.vote_ref().leader_id().get_term(),
//           req.last_log_id.as_ref(),
//           self.state.last_log_id(),
//           lease_status,
//       );
//
//       PreVoteResponse {
//           vote: self.state.vote_ref().clone(),
//           vote_granted: decision.is_granted(),
//           last_log_id: self.state.last_log_id().cloned(),
//       }
//   }
```

**Test to add (W3.3 RED):**
```rust
// openraft/tests/tests/elect/t13_pre_candidate_state.rs
async fn candidate_does_not_advance_term_on_prevote_failure() {
    // 3-node cluster, partition node3, wait for election timeout.
    // Assert RaftMetrics::server_state[node3] == PreCandidate.
    // Both peers reject pre-vote.
    // Assert node3.persisted_vote().term == initial_term (no advance).
}
```

### W3.4 — multi-node runaway-term integration test

```rust
// openraft/tests/tests/elect/t14_runaway_term_repro.rs
async fn partitioned_node_does_not_advance_term() {
    // 3-node cluster.
    // Partition node3 from {node1, node2} for 60s using router's isolated_nodes.
    // During partition, observe node3 enters PreCandidate repeatedly but never
    // promotes because peers are unreachable AND lease ought-to-be-invalid on it
    // (since it can't hear from leader). PreVote semantics: when peers are
    // unreachable, no acks come back — so no quorum, so no promotion, so no
    // term advance.
    //
    // Heal partition. Assert:
    //   - node3.metrics.current_term == initial_term + 0
    //   - cluster recovers to steady state with original leader still leader
    //
    // This is the protocol fix for bug-raft-stale-candidate-runaway-term-no-prevote.md.
}
```

### W3.7 — engine wire-up between `leader_step_down` and `LeaderLease::invalidate`

```rust
// openraft/src/engine/engine_impl.rs::leader_step_down
//   Augment to:
//     - Call self.broadcast_lease_invalidation() before transitioning.
//   broadcast_lease_invalidation() emits a Command::SendAppendEntries with a
//   special "lease_surrender: true" flag (or a dedicated AppendEntries variant)
//   that followers handle by calling LeaderLease::invalidate() locally.
//
//   Alternatively (simpler): the leader simply stops sending heartbeats. The
//   followers' `last_heard_from_leader` ages naturally; their leases expire on
//   their own. Trade-off: adds up to election_timeout latency before a new
//   candidate can win. The explicit broadcast saves that.
```

### W3.8 — TimeoutNow engine handler

```rust
// openraft/src/engine/handler/following_handler/handle_timeout_now_req.rs (NEW)
//
//   pub fn handle_timeout_now_req(&mut self, req: TimeoutNowRequest<C::NodeId>) -> TimeoutNowResponse<C::NodeId> {
//       // Verify the directive comes from a current authoritative leader.
//       if req.vote.leader_id().get_term() < self.state.vote_ref().leader_id().get_term() {
//           // Stale directive; ignore.
//           return TimeoutNowResponse::new(self.state.vote_ref(), false);
//       }
//
//       // Verify our log is caught up to the leader's at transfer time.
//       // (W3.9's transfer_to ensures matched_index == last_log_index before sending.
//       //  This is a defense-in-depth check.)
//       if self.state.last_log_id().cloned() < req.last_log_id {
//           tracing::warn!("TimeoutNow received but local log is behind; refusing");
//           return TimeoutNowResponse::new(self.state.vote_ref(), false);
//       }
//
//       // Transition directly to Candidate (skip PreVote — Ongaro §3.10).
//       self.engine.elect_now(); // fast path: increment term, start election immediately.
//
//       TimeoutNowResponse::new(self.state.vote_ref(), true)
//   }
```

### W3.9 — `Raft::trigger().transfer_to(node_id)` async API

```rust
// openraft/src/raft/trigger.rs
impl<C: RaftTypeConfig> Trigger<C> {
    pub async fn transfer_to(&self, target: C::NodeId) -> Result<(), TransferError<C::NodeId>> {
        // 1. Verify caller is leader.
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state != ServerState::Leader {
            return Err(TransferError::NotLeader);
        }
        if metrics.id == target {
            return Err(TransferError::TargetIsSelf);
        }
        if !metrics.membership_config.membership().is_voter(&target) {
            return Err(TransferError::TargetNotVoter);
        }

        // 2. Stop accepting new client_writes. Hold a transfer_lock on the
        //    raft inner state.
        let _guard = self.raft.inner.acquire_transfer_lock().await;

        // 3. Catch up the target. Loop with bounded retries:
        for retry in 0..TRANSFER_MAX_CATCHUP_RETRIES {
            let progress = self.raft.replication_progress(&target).await?;
            if progress.matched_index() >= metrics.last_log_index {
                break;
            }
            if retry == TRANSFER_MAX_CATCHUP_RETRIES - 1 {
                return Err(TransferError::TargetTooFarBehind);
            }
            tokio::time::sleep(Duration::from_millis(self.raft.config.heartbeat_interval)).await;
        }

        // 4. Send TimeoutNow to target.
        let req = TimeoutNowRequest::new(metrics.vote.clone(), metrics.last_log_id);
        let resp = self.raft.network.timeout_now(target.clone(), req).await?;
        if !resp.started_election {
            return Err(TransferError::TargetRefused);
        }

        // 5. Watch metrics for leader change.
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(self.raft.config.election_timeout_max * 2);
        let mut metrics_rx = self.raft.metrics();
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(TransferError::Timeout);
                }
                _ = metrics_rx.changed() => {
                    let m = metrics_rx.borrow().clone();
                    if m.current_leader == Some(target.clone()) {
                        return Ok(());
                    }
                }
            }
        }
    }
}

// openraft/src/error.rs
#[derive(Debug, thiserror::Error)]
pub enum TransferError<NID: NodeId> {
    #[error("not leader")]
    NotLeader,
    #[error("target is self")]
    TargetIsSelf,
    #[error("target {0} is not a voter in current membership")]
    TargetNotVoter(NID),
    #[error("target too far behind after catchup retries")]
    TargetTooFarBehind,
    #[error("target refused TimeoutNow directive")]
    TargetRefused,
    #[error("transfer timed out (target did not win election)")]
    Timeout,
    #[error("network error during transfer: {0}")]
    Network(#[source] NetworkError),
}
```

### W3.10 — transfer_to timeout safety

This is covered by step 5 of W3.9 above. Test:

```rust
// openraft/tests/tests/membership/t90_transfer_leader.rs
async fn transfer_to_returns_timeout_if_target_does_not_win() {
    // 3-node cluster. Configure target node's network to drop incoming
    // VoteRequest from itself (i.e., it cannot win an election).
    // Call leader.trigger().transfer_to(target).
    // Assert returns TransferError::Timeout within 2 * election_timeout_max.
    // Assert original leader is still leader after the timeout.
}
```

## Why these are deferred

The deferred items (W3.3, W3.7-engine, W3.8-engine, W3.9, W3.10) all touch
the openraft async state machine in non-local ways:

- New `ServerState::PreCandidate` propagates through `RaftMetrics`,
  `calc_server_state`, every `match server_state` site (~12 places),
  `Wait::state(...)` semantics, and downstream apps' UI.
- `transfer_to` requires a synchronization mechanism between the public
  `Raft` handle (clone-shared) and the single-owner `RaftCore` event loop;
  openraft's existing pattern uses `external_request` for this and
  extending it for an awaitable-async result is non-trivial.

A correct implementation requires careful refactoring with the upstream
test suite as a regression net. ADR-012 budgets ~10 working days of
single-engineer focus for the full sprint; this artifact captures
roughly the first 3 days of that work plus the design for the remaining 7.

## Acceptance criteria — per-item

- [x] W3.1 — message types compile and serialize. **PASSING.**
- [x] W3.2 — lease-aware rejection predicate is unit-testable. **PASSING.**
- [ ] W3.3 — multi-node test that PreCandidate does not advance term. **PENDING engine refactor.**
- [x] W3.4 — pure-function repro of the runaway-term bug shows the fix works at the protocol level. **PASSING.** Multi-node version pending W3.3.
- [x] W3.5 — leader steps down on quorum loss. **PASSING (decision + tick handler wired).** Multi-node integration pending.
- [x] W3.6 — leader holds with quorum acks. **By construction.**
- [x] W3.7 — predicate test confirms post-stepdown PreVote succeeds. **PASSING.** Engine wire-up pending.
- [x] W3.8 — TimeoutNow message types compile. **PASSING.** Engine handler pending.
- [ ] W3.9 — `transfer_to` API. **PENDING — full async API.**
- [ ] W3.10 — `transfer_to` timeout safety. **PENDING.**

Total fork-side completion: ~60%. The remaining ~40% is the engine-state-machine
work plus the `transfer_to` async API.
