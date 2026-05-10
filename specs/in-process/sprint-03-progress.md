---
type: progress
sprint: 3
created: 2026-05-09
last-updated: 2026-05-09
---

# Sprint 3 Progress — openraft fork: PreVote, CheckQuorum, Leadership Transfer

## Repos and branches used

- **openraft fork** (local clone of upstream, prepared for operator push):
  `/home/bkearns/src/ferrosa-openraft/`. Branch `correctness/prevote-checkquorum`
  off `ferrosadb/fix/separate-replication-timeout`. Three commits below.
- **ferrosa worktree**: `/home/bkearns/src/ferrosa-suite/sprint-03-openraft-fork/`.
  Branch `sprint-03-openraft-fork`. One commit.

## Per-work-item status

| WI    | Status   | Repo / Commit | Notes |
|-------|----------|---------------|-------|
| W3.1  | **Done (surface)** | openraft `1048f911` | Message types `PreVoteRequest`/`Response`, trait method, `RPCTypes` variants, `Config::enable_pre_vote`. Full unit test coverage. Engine handler `handle_pre_vote_req` is **deferred** to a follow-up commit (see W3.3). |
| W3.1 REFACTOR | **Done** | openraft `437e37d8` | `is_log_up_to_date` extracted as pure-function predicate shared between Vote and PreVote paths. |
| W3.2  | **Done (predicate)** | openraft `437e37d8` | `LeaderLease::is_active(elapsed)` + `evaluate_pre_vote(...)` cover lease-aware rejection. Engine wire-up deferred. |
| W3.3  | **Deferred** | — | Requires new `ServerState::PreCandidate`, refactor of `calc_server_state`, new `PreCandidateState` proposer module. ~12 match-site updates. Full pseudo-code in `sprint-03-openraft-patches.md`. |
| W3.4  | **Done (predicate)** | openraft `437e37d8` | `w3_4_runaway_term_repro_partitioned_candidate_with_stale_log` test proves protocol fix at decision-function level. Multi-node integration test pending W3.3. |
| W3.5  | **Done** | openraft `58365ff3` | `CheckQuorum` decision struct, `LeaderData::elected_at`, `RaftCore::handle_tick_check_quorum(now)` wired into `Notify::Tick`. 7 unit tests including the ferrosa default at ratio=0.75. |
| W3.5 REFACTOR | **Partial** | openraft `437e37d8` | `LeaderLease` type extracted but not yet stored on `LeaderData`. |
| W3.6  | **Done by construction** | openraft `58365ff3` | `Healthy` returned whenever `elapsed < deadline`; covered by `healthy_when_recent_ack`. |
| W3.7  | **Done (predicate)** | openraft `437e37d8` | `pre_vote_granted_after_lease_invalidation_w3_7` test proves the post-stepdown PreVote contract. Engine wire-up between `leader_step_down` and `LeaderLease::invalidate` deferred. |
| W3.8  | **Done (surface)** | openraft `1048f911` | `TimeoutNowRequest`/`Response`, `RaftNetwork::timeout_now` default, `RPCTypes::TimeoutNow`. Engine handler deferred. |
| W3.9  | **Deferred** | — | `Raft::trigger().transfer_to(node_id)` async API needs an external_request extension to await metric changes from the public handle. Full pseudo-code in proposal doc. |
| W3.10 | **Deferred** | — | Trivial once W3.9 lands. |
| W3.11 | **Done** | ferrosa `3011200d` | `Cargo.toml` `[patch.crates-io]` repointed to local fork path. Full workspace builds clean. |
| W3.12 | **Partial** | ferrosa `3011200d` | Knobs (`raft_enable_pre_vote`, `raft_check_quorum_ratio`) added to `ClusterConfig`, env-var-overridable, plumbed into `controller/cluster.rs:840`. Acceptance test `ferrosa_partitioned_node_does_not_advance_term` written and gated on `--features sprint-03-engine-prevote`. **Test currently fails by design** because the engine-side PreVote handler is not yet wired — failure message points to the deferred work item. |
| W3.13 | **Done (CLI)** | ferrosa `3011200d` | `ferrosa-ctl raft transfer-leader --to <host_id>` parses correctly and hits the `POST /api/cluster/raft/transfer-leader` endpoint. Server-side handler depends on W3.9, so the command surfaces a clean "not yet implemented" HTTP 404/501 today. |

## Test results

### openraft fork (`/home/bkearns/src/ferrosa-openraft`)

```
$ cargo test -p openraft --lib
test result: ok. 214 passed; 0 failed; 0 ignored; 0 measured

$ cargo test --test elect
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured
```

New tests added by this sprint:
- `engine::check_quorum::tests::*` (7 tests)
- `engine::leader_lease::tests::*` (7 tests)
- `engine::pre_vote_decision::tests::*` (14 tests including W3.4 repro)
- `config::config_test::test_config_enable_pre_vote` (1 test)
- `config::config_test::test_config_check_quorum_ratio_*` (3 tests)
- `tests/elect/t12_pre_vote_basic::*` (2 tests)

= **34 new tests**, all green.

### ferrosa worktree (`sprint-03-openraft-fork`)

```
$ cargo test -p ferrosa-cluster --lib
test result: ok. 661 passed; 0 failed; 0 ignored

$ cargo test -p ferrosa-cluster --test raft_election_storm
test result: ok. 3 passed; 0 failed; 0 ignored

$ cargo test -p ferrosa-ctl --bin ferrosa-ctl
test result: ok. 103 passed; 0 failed; 0 ignored
```

New tests added by this sprint (ferrosa side):
- `config::tests::default_raft_correctness_knobs_match_adr_012` (asserts ferrosa
  defaults match ADR-012: pre_vote=true, check_quorum_ratio=0.75)
- `tests::parse_raft_transfer_leader` (CLI parse test)
- `ferrosa_partitioned_node_does_not_advance_term` (gated; demonstrates gap)

## Commits

```
$ cd /home/bkearns/src/ferrosa-openraft
$ git log --oneline correctness/prevote-checkquorum ^ferrosadb/fix/separate-replication-timeout
437e37d8 feat: PreVote/Vote decision predicates + LeaderLease (W3.1 refactor, W3.2, W3.4, W3.7)
58365ff3 feat: CheckQuorum step-down decision + tick handler (W3.5, W3.6)
1048f911 feat: PreVote/TimeoutNow message types + CheckQuorum config (W3.1, W3.5, W3.8 surface)
```

```
$ cd /home/bkearns/src/ferrosa-suite/sprint-03-openraft-fork
$ git log --oneline -1
3011200d sprint-03(adr-012): repoint openraft, expose PreVote/CheckQuorum knobs, raft transfer-leader CLI
```

## Work remaining for sprint completion

The deferred items (W3.3, W3.7-engine, W3.8-engine, W3.9, W3.10) require
extending the openraft async state machine in non-local ways. Pseudo-code
is in `sprint-03-openraft-patches.md`. Estimate: ~7 more engineer-days
(matching the original ADR-012 budget — this sprint accomplished ~3 days
of foundation work plus the design for the remaining 7).

The handoff is clean: every protocol decision is captured in a unit-tested
pure-function module. The engine wire-up just calls these decision functions
at the right places and applies their results.

## Next steps for an operator

1. Push `correctness/prevote-checkquorum` from
   `/home/bkearns/src/ferrosa-openraft` to `github.com/ferrosadb/openraft`.
2. Update ferrosa `Cargo.toml` `[patch.crates-io]` to use the git form once
   pushed (the local-path form is for development).
3. Open upstream PRs as ADR-018 specifies: CheckQuorum and Leadership
   Transfer are upstreamable; PreVote will be carried fork-only per the
   author's stated position.
4. Schedule the engine-state-machine work (W3.3, W3.7-engine, W3.8-engine,
   W3.9, W3.10) on a separate sprint with a dedicated engineer per ADR-012's
   original budget.
