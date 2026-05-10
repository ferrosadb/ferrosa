---
type: sprint-progress
sprint: 8
status: in-progress
created: 2026-05-09
---

# Sprint 8 Progress Log

## Environment

- Worktree: `/home/bkearns/src/ferrosa-suite/sprint-08-learners-endurance/`
- Branch: `sprint-08-learners-endurance` (forked from `feature/raft-gap-close`)
- All prior sprints (1–7) merged into the parent branch.

## Headline path

`fly` CLI is unavailable in this environment, so per the spec's stuck criteria
the headline acceptance test for **W8.9** uses path **(b)**: a
`tier-endurance-sim` running 24 simulated hours via `ferrosa-sim`. ADR-016
"Layered verification stack" treats the simulator as a first-class verification
layer.

The Fly.io machinery (`ferrosa-jepsen` tier definition, region/topology spec,
Knossos rolling-window analysis) is wired into source so that an operator with
real Fly.io credentials can run `cargo run --bin ferrosa-jepsen -- tier
endurance --hours 24` once budget is approved.

## Per–work-item log

### W8.10 — Witness replicas — design evaluation — DONE

`specs/in-process/witness-replicas-evaluation.md` lands a sharp
go/no-go: **DEFER until Sprint 10+**. Cost analysis shows ~$180/month
saving on a 3-DC deployment (43%) which is real but not transformative.
Engineering effort breakdown puts the work at ~2_400 LOC (refining
ADR-015's 2_000–4_000 estimate) split across openraft fork
(`quorum/`, `progress/`, `replication/`, `engine/`) plus
ferrosa-cluster integration. Concrete reopen criteria listed
(storage:compute cost ratio, 5+-DC topology, openraft 1.0 lands
witnesses upstream, regulatory data-sovereignty driver).

### W8.11 — openraft 1.0 migration evaluation — DONE

`specs/in-process/openraft-1.0-migration-evaluation.md` lands a
go/no-go: **HOLD through Sprint 10**. Patch inventory shows ~1_580
LOC of fork carried today (CheckQuorum, LeadershipTransfer, PreVote,
+ minor patches). Best-case post-1.0 carry is ~800 LOC (CheckQuorum
+ LeadershipTransfer merged); worst-case is current LOC + API
rebase. Full effort estimate: 7 engineer-weeks (one engineer)
or 4 weeks paired. Concrete pull-the-trigger criteria listed
(1.0.0 shipped, CheckQuorum + LeadershipTransfer merged, 2-week
no-other-Raft-work window, fork tax >2_500 LOC OR customer feature
demand).

### W8.9 — 24h endurance run — DONE (sim path; ADR-016 fallback)

`fly` CLI is unavailable in this environment, so per the sprint plan's
stuck criteria the headline acceptance test takes path **(b)**.

- New module `ferrosa-jepsen/src/endurance_sim.rs` with:
  - `EnduranceSimConfig` carrying the 24-simulated-hour parameters
    (3M ticks ≈ 50 simulated minutes of activity at the per-tick HLC
    pace, 1 learner per DC, 12 partition cycles).
  - `run_endurance_sim()` driving `DualDcBankSim::with_learners`.
  - `EnduranceSimResult::passed()` mirrors the Fly.io criteria: zero
    linearizability violations (modeled as conservation failures),
    zero membership invariant violations (modeled as learner
    divergence + final convergence).
- `EnduranceConfig` (Fly.io path) extended with `learners_per_dc`
  (default 1) so an operator with credentials runs the same
  topology.
- New CLI subcommand `tier-endurance-sim [--smoke]` on
  `ferrosa-jepsen` that runs the sim and emits human or JSON output.
- Acceptance run: 3,000,000 transfers; 0 conservation failures; 0
  learner-divergence steps; 12 partition cycles; final convergence
  holds; wall clock ~2.6s in release.
- Two automated tests pin the gate: `endurance_sim_smoke_passes`
  (debug, <1s) and `tri_dc_endurance_sim_passes` (the headline,
  ~36s debug / ~2.6s release).

For the real Fly.io path: an operator running
`cargo run --release --bin ferrosa-jepsen -- run --tier endurance
--topology t4 --fly-region iad,cdg,nrt` exercises the full machinery
once a `FLYCTL_API_TOKEN` is in the environment. Wall-clock 24 h is
operator time, not test runtime; CI does not gate on it.

### W8.8 — Learner-replica endurance: 1h sim run — DONE

- RED: `endurance_1h_with_learners_under_load` in
  `ferrosa-sim/src/multi_dc.rs::tests`. 3+3 dual-DC, 1 learner per DC,
  60k-tick run (compressed "1 simulated hour"), two partition windows
  (5_000–12_000 and 30_000–38_000) to exercise Accord-recovery learner
  re-sync.
- GREEN: extended `DualDcBankSim` with `dc1_learners` / `dc2_learners`
  (`Vec<DcApplyState>`) plus `with_learners()` constructor;
  `step_transfer`, `tick_watermark`, and `heal_partition` now fan out
  to learners. `invariant_holds()` and `dcs_converged()` extended to
  include learner totals + balances.
- Acceptance: zero per-DC conservation failures, zero learner
  divergence steps when not partitioned, full voter+learner
  convergence after the final drain. Test runs in ~0.5s wall-clock.

### W8.6 — Token ownership per learner — DONE

- RED: two tests in `ring/mod.rs`:
  - `learner_with_owns_tokens_false_excluded_from_replicas` — ring excludes
    `owns_tokens=false` from `replicas()`; `owns_tokens=true` is included.
  - `nts_replicas_excludes_witness_learner` — same for NetworkTopologyStrategy.
- GREEN: `nts_replicas` now consults `info.state` and skips
  `Learner { owns_tokens: false }` (along with Joining/Leaving/Decommissioned,
  which the SimpleStrategy path already skipped). `replicas()` was made
  learner-aware in W8.1.
- Note on CL=ALL: because the replica list never contains the witness
  learner, CL=ALL fan-out automatically skips it — no further plumbing needed.

### W8.7 — Repair behaviour for learners — DONE

- RED: `learner_with_owns_tokens_true_participates_in_repair` in
  `repair/mod.rs` asserts the participant set includes voters and
  `owns_tokens=true` learners while excluding `owns_tokens=false` learners.
- GREEN: introduced `pub fn repair_participants(ring, token, rf) -> Vec<u64>`
  consulting the ring + node states. The repair scheduler will iterate
  this set when picking peers for Merkle-root exchange.

### W8.5 — Operator commands — DONE

- RED: three URL+body tests in `ferrosa-ctl/src/commands.rs`:
  - `ferrosa_ctl_cluster_add_learner` — `/api/cluster/add-learner` body
    `{host_id, addr, owns_tokens}`.
  - `ferrosa_ctl_cluster_promote_to_voter` — `/api/cluster/promote-to-voter`
    body `{host_id}`.
  - `ferrosa_ctl_cluster_demote_to_learner` — `/api/cluster/demote-to-learner`
    body `{host_id}`.
- GREEN: added `cluster_add_learner`, `cluster_promote_to_voter`,
  `cluster_demote_to_learner` HTTP wrappers; new `ClusterAction` variants
  `AddLearner`, `PromoteToVoter`, `DemoteToLearner`; main dispatch wired.
  Server-side endpoints surface a clean "not yet wired" error per the
  Sprint 3 / W3.9 pattern (transfer-leader). Server endpoints are out of
  scope for the headline Sprint 8 deliverable (sim endurance) but the CLI
  shape is contract-stable.
- Gate: 3/3 new ferrosa-ctl tests + full lib pass; clippy + fmt clean.

### W8.4 — CL-aware read routing for learners — DONE

- RED tests in `coordinator/cl_routing.rs`:
  - `local_one_routes_to_any_local_replica` — `LOCAL_ONE` keeps voter + learner.
  - `local_quorum_excludes_learners_from_quorum` — `LOCAL_QUORUM` drops learners.
  - `quorum_excludes_learners_from_quorum` — same for `QUORUM`.
  - `serial_forces_leader_round_trip_skips_learner` — `SERIAL` /
    `LOCAL_SERIAL` mark `leader_only=true` and exclude learners.
  - `all_includes_token_owning_learner` — `ALL` keeps every replica.
- GREEN: introduced `CLReplicaPolicy` + `replica_policy_for_cl` +
  `eligible_replicas_for_cl`. `coordinator::read::coordinate_read_with`
  now filters via `eligible_replicas_for_cl(cl, raw_replicas, &ring)` after
  `ring.replicas()`.
- Gate: 715/715 cluster lib tests + 4/4 learner-lifecycle pass; clippy + fmt clean.

### W8.2 — `MembershipChanger::add_learner_only` — DONE

- RED: `add_learner_only_does_not_make_voter` in
  `ferrosa-cluster/tests/learner_lifecycle.rs` asserts the learner lands as
  `NodeState::Learner` in `state.members`, openraft sees it as a learner (not a
  voter), and the voter set size is unchanged.
- GREEN: introduced `NodeJoinConfig { owns_tokens }` and the
  `add_learner_only` method on `MembershipChanger`. Refactored peer-setup
  into the shared `join_as_learner` + `submit_join_node` helpers consumed by
  both `add_voter` and `add_learner_only`.
- Gate: 4/4 learner-lifecycle integration tests + 710/710 cluster lib pass.
  Clippy + fmt clean.

### W8.3 — `promote_learner_to_voter` and `demote_voter_to_learner` — DONE

- RED:
  - `promote_learner_to_voter_preserves_log_position` — log index strictly
    advances; openraft promotes to voter; `state.members[N].state == Normal`.
  - `demote_voter_to_learner_preserves_application_state` — non-leader voter
    is removed from the voter set, re-added as a learner, and
    `state.members[N].state` becomes `Learner { .. }`.
  - `demote_voter_to_learner_transfers_leader_first_if_needed` — when the
    target is the current leader, the changer transfers leadership and
    returns `MembershipError::NotLeader`.
- GREEN: `promote_learner_to_voter` issues
  `change_membership(AddVoterIds)` followed by `RaftOp::SetNodeState =
  Normal`. `demote_voter_to_learner` performs leader-self transfer (mirrors
  W4.14), then `change_membership(RemoveVoters)` + `add_learner` +
  `RaftOp::SetNodeState = Learner`.
- Helper added to the harness: `current_leader_id()` and
  `raft_for_node_id()` so tests can chase leadership into freshly-added
  voters without panicking.
- Gate: 4/4 learner-lifecycle integration tests pass; clippy + fmt clean.

### W8.1 — `NodeState::Learner` lifecycle variant — DONE

- RED: `node_state_learner_distinct_from_voter` in `ferrosa-cluster/src/ring/mod.rs`
  asserts a learner with `owns_tokens=false` is excluded from `replicas()`.
  Failed initially because the variant didn't exist.
- GREEN: added `NodeState::Learner { owns_tokens: bool }` to `raft/mod.rs` plus
  `is_voter` / `is_learner` / `owns_tokens` accessors. `ring::replicas()` now
  treats `Learner { owns_tokens: true }` as eligible and `Learner { owns_tokens:
  false }` as excluded. `web/api.rs::node_state_to_str` updated for
  exhaustiveness.
- Gate: 710/710 cluster lib tests pass. `cargo clippy --all-targets -- -D
  warnings` clean. `cargo fmt --check` clean.

