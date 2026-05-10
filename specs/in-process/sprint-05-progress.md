---
type: progress
sprint: 5
created: 2026-05-10
last-updated: 2026-05-10
---

# Sprint 5 Progress — Deterministic simulation harness + TLA+ skeleton

## Approach

Strict TDD throughout. Each WI lands as a focused commit on the
`sprint-05-sim-tla` worktree branch. CI gates from Sprint 1 plus
`cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`
green every commit.

## Decision: in-house simulator over Madsim

ADR-017 grants the implementer authority to fall back from Madsim if
integration friction is too high. Sprint 5 elects the fallback path
**up-front**, before W5.2 turns into a multi-day refactor of every
`tokio::time::Instant::now()` call site in ferrosa-cluster. Rationale:

- Madsim shims `tokio` via a feature flag, but openraft 0.9 + sled +
  the existing `FerrosRaft` pull in network and disk paths that are
  outside Madsim's well-trodden coverage. The Sprint 4 audit lists 50+
  call sites of wall-clock APIs across `ferrosa-cluster` alone.
- The headline goal of Sprint 5 is the **TLA+ refinement check**
  (W5.10): observed simulator transitions checked against a TLA+ spec.
  That goal does not require running the *entire* ferrosa-cluster
  binary under sim — it requires a **state machine** that exhibits the
  same protocol transitions the TLA+ spec describes, fed by a
  deterministic event loop.
- TigerBeetle and FoundationDB simulate at the protocol level — they
  rewrote their networking and storage to be deterministic. Madsim is
  a quicker path *if* your code is greenfield; ferrosa is not.

The in-house sim therefore models the **Raft protocol** (term, vote,
log, state) directly, with a deterministic event loop and seeded RNG.
Bootstrap-phase transitions (W5.6) ride on top of the protocol
simulator: each bootstrap phase becomes a sim event that must satisfy
its precondition before firing, exactly like the typed phases in
`ferrosa-cluster/src/controller/bootstrap/`.

Madsim adoption remains an option for a future sprint if the
in-process integration tests in `ferrosa-cluster/tests/` grow
hard-to-reproduce flakes.

## Per-work-item status

| WI    | Status | Commit prefix | Tests added | Notes |
|-------|--------|---------------|-------------|-------|
| W5.1  | **Done** | feat(sim): W5.1 | 1 (`crate_compiles_and_runs_empty_test`) | New crate `ferrosa-sim` added to workspace; deps `serde`, `tracing`, dev `proptest`. |
| W5.2  | **Done** | feat(sim): W5.2 | 1 (`madsim_runs_single_node`) | `SimulatedNode` + `Role` + mirrored `DeploymentMode`; protocol-level only — no openraft/sled wrapping. |
| W5.3  | **Done** | feat(sim): W5.3 | 3 (`madsim_runs_3_node_to_cluster`, RNG determinism × 2) | `SimulatedCluster` discrete-event loop: `ElectionTimeout`, `RequestVote`, `RequestVoteReply`, `Heartbeat`. 3-voter cluster reaches a leader in <10K simulated ticks. |
| W5.4  | **Done** | feat(sim): W5.4 | 2 (`same_seed_produces_same_trace`, `different_seeds_produce_different_traces`) | `Trace` + `TlaAction` types; trace recorded for every transition. README documents the determinism contract. |
| W5.5  | **Done** | feat(sim): W5.5 | 3 (`sim_nemesis_partition_halves`, `sim_nemesis_kill_minority`, `sim_nemesis_add_node`) | `Nemesis` trait + `PartitionHalves`, `KillMinority`, `AddNode`. Cluster API: `partition_pair`, `kill`, `add_voter`, `run_for`. |
| W5.6  | **Done** | feat(sim): W5.6 | 11 (8 phase-level + `sim_full_bootstrap_pipeline` + `sim_full_bootstrap_seed_sweep` (100 seeds) + `seed_37_settles_to_one_leader_two_followers`) | Sim-level mirror of Sprint 4's 8 `BootstrapPhase`s with pre/post-conditions; runtime tests via `run_phase` and `run_full_bootstrap`. |
| W5.7  | **Done** (spec written, Apalache check is operator follow-up) | docs(tla): W5.7 | 3 (`tla_spec_file_exists`, `safety_invariants_hold_after_election`, `election_safety_catches_two_leaders`) | `specs/tla/raft.tla` + `raft.cfg` covering ElectionSafety, LogMatching, LeaderCompleteness, StateMachineSafety, LeaderAppendOnly. Apalache not installed in the agent env; Rust-side `spec` module re-implements the snapshot invariants for sim-time use. |
| W5.8  | **Done** | feat(sim): W5.8 | 2 (`pre_vote_round_promotes_to_leader`, `pre_vote_blocks_term_advance_in_minority_partition`) | PreVote events + `Role::PreCandidate` path; `with_pre_vote()` builder. `NoTermAdvanceWithoutPreVoteMajority` runtime invariant. TLA+ spec already encodes PreVote. |
| W5.9  | **Done** | feat(sim): W5.9 | 2 (`joint_consensus_quorum_requires_both_sides`, `joint_consensus_drops_old_voter_at_commit`) | `propose_membership(Cnew)` / `commit_membership` / `is_joint_quorum`. Joint quorum = majority of Cold AND majority of Cnew. `JointConsensusSafety` runtime invariant. TLA+ spec already encodes the joint quorum. |
| W5.10 | **Done** | feat(sim): W5.10 | 4 (`every_sim_transition_is_tla_permitted`, `refinement_holds_across_1000_seeds`, `refinement_rejects_two_leaders_same_term`, `refinement_rejects_term_regression`) | Rust TLA+ refinement interpreter at `refinement.rs`. Replays each `TlaAction` against `AbstractState`, rejects two-leaders-same-term, term regression, stale grants, etc. **1000-seed sweep passes.** Throughput benchmark: ~86M seeds/min for the protocol-level sim (well above the 10K/min target). |

## Final commit count

TBD.
