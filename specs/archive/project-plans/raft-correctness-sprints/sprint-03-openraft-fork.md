---
type: sprint
status: pending
priority: P0
created: 2026-05-09
sprint: 3
wave: 1
---

# Sprint 3: Fork openraft, add PreVote + CheckQuorum + Leadership Transfer

> Branch in fork repo: `correctness/prevote-checkquorum` off `ferrosadb/0.9` of `github.com/ferrosadb/openraft`.
> Branch in ferrosa: `sprint-03-openraft-fork-integration` off `main`.
> Companion to: ADR-012 (the patch set), ADR-018 (fork policy).

## Goal

Land PreVote, CheckQuorum, and Leadership Transfer in the ferrosa-owned openraft fork. Repoint ferrosa Cargo.toml. Verify the open `bug-raft-stale-candidate-runaway-term-no-prevote.md` no longer reproduces. File CheckQuorum and Leadership Transfer upstream as PRs.

## Hard dependencies

None — wave 1. Cross-repo: this sprint is mostly in `ferrosadb/openraft` with a small ferrosa-side integration commit.

## Pre-flight checks

```sh
# Set up fork
gh repo create ferrosadb/openraft --public --source upstream --remote-clone || true
cd ferrosadb-openraft
git remote add upstream https://github.com/databendlabs/openraft.git
git fetch upstream
git checkout -b ferrosadb/0.9 upstream/release-0.9.24
# Squash existing fix/separate-replication-timeout patches onto ferrosadb/0.9
git cherry-pick <existing-fork-commits>
cargo test                                    # baseline green
git checkout -b correctness/prevote-checkquorum
```

## TDD work items (fork-side)

Each work item: write the failing test in the fork's test suite, watch it fail, implement minimally, refactor.

### W3.1: PreVote — RPC and trait method

**RED.** New test `tests/prevote_basic.rs::pre_vote_request_returns_grant_for_up_to_date_candidate`. Uses openraft's existing test harness; sends a `PreVoteRequest` to a Follower; expects `PreVoteResponse { vote_granted: true }`. Compile error: `PreVoteRequest` doesn't exist.

**GREEN.**
- New types `PreVoteRequest`, `PreVoteResponse` (mirror `VoteRequest`/`VoteResponse` but with `is_prevote: true` semantics).
- New trait method `RaftNetwork::pre_vote(req) -> Result<PreVoteResponse, RPCError>`. Default impl returns `Unimplemented` to avoid breaking downstream impls.
- Engine handler: `handle_pre_vote` mirrors `handle_vote` but does **not** mutate `Vote` state.

**REFACTOR.** Extract the election-restriction predicate (`candidate.last_log_id >= self.last_log_id`) into a shared helper called from both `handle_vote` and `handle_pre_vote`.

### W3.2: PreVote — lease-aware rejection

**RED.** Test `pre_vote_rejected_when_leader_lease_active`: voter has heard from leader within `election_timeout`; receives `PreVoteRequest`; responds `vote_granted: false`. Currently fails — no lease check in W3.1's stub.

**GREEN.** In `handle_pre_vote`, check `self.last_heard_from_leader + election_timeout > now`. If yes, reject.

**REFACTOR.** None.

### W3.3: PreVote — no term advance until majority pre-grants

**RED.** Test `candidate_does_not_advance_term_on_prevote_failure`: a node enters PreCandidate state; sends `PreVoteRequest` to two peers; both reject; node's persisted `Vote.term` is unchanged.

**GREEN.** New `ServerState::PreCandidate`. Election state machine: `Follower → PreCandidate → Candidate → Leader`. Transition `PreCandidate → Candidate` only on pre-vote majority; transition back to Follower (without term bump) on pre-vote rejection.

**REFACTOR.** Update `RaftMetrics::server_state` to include `PreCandidate`.

### W3.4: PreVote — repro the runaway-term bug

**RED.** Test `partitioned_node_does_not_advance_term`: 3-node cluster; partition node3 from {node1, node2} for 60s; reconnect; assert node3's persisted term has advanced by **at most 0** (PreVote should suppress every attempt).

**GREEN.** This is a regression test for the Ferrosa-side bug `bug-raft-stale-candidate-runaway-term-no-prevote.md`. With W3.1–W3.3 in place, no GREEN is needed beyond running it. If it fails, debug PreVote.

**REFACTOR.** None.

### W3.5: CheckQuorum — basic stepdown

**RED.** Test `leader_steps_down_on_quorum_loss`: 3-node cluster; leader cannot reach either follower for `election_timeout × 0.75` (the ferrosa-default ratio); assert leader transitions to Follower; `RaftMetrics::server_state` reflects.

**GREEN.**
- New tick handler in the leader engine: every `heartbeat_interval`, check `last_quorum_ack_timestamp + election_timeout * check_quorum_ratio < now`. If yes, transition to Follower.
- `check_quorum_ratio` is part of `Config`; default 1.0 in upstream openraft, 0.75 in ferrosa Cargo.toml override.
- Clear `Vote.voted_for` and surrender lease on stepdown.

**REFACTOR.** Pull lease tracking into a `LeaderLease` type in `engine/`.

### W3.6: CheckQuorum — does not step down with quorum acks

**RED.** Test `leader_holds_with_quorum_acks`: 3-node cluster; leader receives AppendEntries acks every heartbeat from at least one follower; runs for `election_timeout × 5`; assert leader stays leader.

**GREEN.** Already passes if W3.5 is correct.

**REFACTOR.** None.

### W3.7: CheckQuorum — interaction with PreVote

**RED.** Test `prevote_succeeds_after_checkquorum_stepdown`: trigger CheckQuorum stepdown on leader N1; on partition heal, N2 (was follower) starts PreVote; PreVote majority because N1 is now follower (lease surrendered); N2 wins election cleanly.

**GREEN.** Verify the `last_heard_from_leader` logic: when leader steps down via CheckQuorum, it must surrender its lease so PreVote-grant from `last_heard_from_leader + election_timeout > now` becomes false on followers. Add a "lease invalidated" marker.

**REFACTOR.** None.

### W3.8: Leadership Transfer — TimeoutNow RPC

**RED.** Test `timeout_now_rpc_starts_election`: send `TimeoutNow { vote, last_log_id }` to a Follower; assert it immediately transitions to Candidate (not PreCandidate — explicit transfer skips PreVote per ADR-012); election starts at `current_term + 1`.

**GREEN.**
- New types `TimeoutNowRequest`, `TimeoutNowResponse`.
- New `RaftNetwork::timeout_now(req)` trait method.
- Engine handler: on receipt, if `req.vote.term >= self.current_term`, advance term, transition to Candidate, start election.

**REFACTOR.** None.

### W3.9: Leadership Transfer — `transfer_to` API

**RED.** Test `transfer_to_makes_target_leader`: 3-node cluster, leader N1, call `raft.trigger().transfer_to(N2).await`; assert within `election_timeout × 2` that N2 is leader.

**GREEN.**
- `Raft::trigger().transfer_to(node_id)`: stops new client_writes; ensures target's `replication.matched_index == last_log_index` (catches up via AppendEntries with bounded retries); sends `TimeoutNow`.
- New error variant `TransferError::TargetTooFarBehind`.

**REFACTOR.** None.

### W3.10: Leadership Transfer — timeout safety

**RED.** Test `transfer_to_returns_timeout_if_target_does_not_win`: configure target to drop incoming votes; call `transfer_to`; assert returns `TransferError::Timeout` within `election_timeout × 2`; original leader resumes leadership.

**GREEN.** `transfer_to` watches `metrics().server_state`; if no Leader change within `election_timeout × 2`, returns Timeout and resumes.

**REFACTOR.** None.

## TDD work items (ferrosa-side integration)

After fork patches land:

### W3.11: Repoint ferrosa Cargo.toml

**RED.** Run `cargo test --workspace --lib`. Currently green against the existing fork branch.

**GREEN.** Update `Cargo.toml`:
```toml
openraft = { git = "https://github.com/ferrosadb/openraft.git", branch = "ferrosadb/0.9", features = ["serde", "storage-v2", "loosen-follower-log-revert"] }
```
Run tests; expect green.

**REFACTOR.** Update lockfile; commit.

### W3.12: Enable PreVote and CheckQuorum in ferrosa

**RED.** Test `ferrosa_partitioned_node_does_not_advance_term`: same as W3.4 but at the ferrosa-cluster level using the existing test harness.

**GREEN.** In `controller/cluster.rs:840-851` openraft `Config`, set `enable_pre_vote: true` and `check_quorum_ratio: 0.75` (knobs newly exposed by the fork).

**REFACTOR.** Plumb `check_quorum_ratio` to the `FERROSA_RAFT_CHECK_QUORUM_RATIO` env var; default 0.75.

### W3.13: Wire `ferrosa-ctl raft transfer-leader`

**RED.** Test `ferrosa_ctl_transfer_leader`: 3-node cluster; run `ferrosa-ctl raft transfer-leader --to <host_id>`; assert leader changes within 500ms; zero failed writes during the window.

**GREEN.** Add the subcommand. Calls `raft.trigger().transfer_to(target_node_id).await`.

**REFACTOR.** None.

## Acceptance criteria (sprint-level)

Fork-side:
- [ ] `ferrosadb/openraft` repo exists with `ferrosadb/0.9` branch.
- [ ] PreVote tests pass: W3.1, W3.2, W3.3, W3.4.
- [ ] CheckQuorum tests pass: W3.5, W3.6, W3.7.
- [ ] Leadership Transfer tests pass: W3.8, W3.9, W3.10.
- [ ] PRs filed upstream for CheckQuorum (PR #N) and Leadership Transfer (PR #M). PreVote may be filed but expect rejection.

Ferrosa-side:
- [ ] `cargo test --workspace --lib` green against the fork.
- [ ] `ferrosa_partitioned_node_does_not_advance_term` (W3.12) passes — proves the open in-process bug is fixed.
- [ ] `ferrosa_ctl_transfer_leader` (W3.13) passes.
- [ ] `bug-raft-stale-candidate-runaway-term-no-prevote.md` moved from `specs/in-process/` to `specs/implemented/` with a brief implementation note.

## Parallelization within Sprint 3

- **Track A (PreVote)**: W3.1, W3.2, W3.3, W3.4 — serialize within track.
- **Track B (CheckQuorum)**: W3.5, W3.6, W3.7 — independent of A until W3.7 (which combines both).
- **Track C (Leadership Transfer)**: W3.8, W3.9, W3.10 — independent.
- **Ferrosa integration (W3.11–W3.13)**: only after fork branch is green.

A 3-engineer team finishes the fork side in ~1.5 weeks.

## Risks

- **R1 — Upstream openraft author rejects PreVote PR**: expected. Carry as fork-only.
- **R2 — Engine refactor for PreCandidate state breaks existing tests**: openraft has substantial test coverage. Mitigation: run upstream tests after each commit; revert any that regress without explanation.
- **R3 — `loosen-follower-log-revert` interacts unexpectedly with new state machine paths**: mitigation: add a test that verifies log revert never fires in a successful PreVote/CheckQuorum scenario.
- **R4 — `single-term-leader` cargo feature path breaks**: per ADR-012 we don't enable it; openraft's test suite may still exercise it. Mitigation: keep tests green for both feature configurations.

## Completion checklist

- [ ] Both PRs (fork + ferrosa) merged.
- [ ] CockroachDB-style adversarial test (asymmetric partition) confirms <500ms recovery.
- [ ] Coordinator file updated.

## Kickoff prompt for an agent

> You are executing Sprint 3 of the Ferrosa Raft Correctness Program. Spec at `/home/bkearns/src/ferrosa-suite/raft-correctness/specs/in-process/sprint-03-openraft-fork.md`.
>
> This sprint is **cross-repo**. You will work in two places:
> 1. Fork repo `github.com/ferrosadb/openraft` (create if it doesn't exist; baseline = openraft 0.9.24 release with existing `fix/separate-replication-timeout` patches).
> 2. Ferrosa repo at `/home/bkearns/src/ferrosa-suite/ferrosa/` for the integration commit.
>
> Execute W3.1–W3.13 in TDD order. Each work item: RED → GREEN → REFACTOR. Each commit ends green. Branch in fork: `correctness/prevote-checkquorum`. Branch in ferrosa: `sprint-03-openraft-fork-integration`.
>
> Companion reading: ADR-012 (the protocol-level spec), ADR-018 (fork policy), Ongaro dissertation §3.10/§6.4/§9.6 for the protocol details.
>
> Constraints: strict TDD; do not break upstream openraft tests; PreVote may be rejected upstream — file the PR but proceed with carrying as fork-only.
>
> When the sprint completes, file an issue or PR in the upstream openraft repo for CheckQuorum and Leadership Transfer; mark PreVote as fork-only.
