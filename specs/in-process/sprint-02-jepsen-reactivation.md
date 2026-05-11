---
type: sprint
status: pending
priority: P0
created: 2026-05-09
sprint: 2
wave: 1
---

# Sprint 2: Jepsen reactivation + structural invariants

> Branch: `sprint-02-jepsen-reactivation` off `main` of inner ferrosa repo.
> Companion to: ADR-016 (Verification stack), Agent C's audit findings.

## Goal

Convert ferrosa-jepsen from theatre to a working test harness that runs on every PR and would have caught every recent membership/forwarding bug as a regression. Three blockers (orchestrator wiring, CI exclusion, asymmetric seed config), six structural invariants, three topology nemeses, three workloads.

## Hard dependencies

None — wave 1. The Sprint 1 `MembershipChanger` makes the structural invariants meaningful, but Sprint 2 work proceeds against the existing API and gains its full value once Sprint 1 lands. **Parallel-safe with Sprint 1.**

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
cargo test -p ferrosa-jepsen --lib                # baseline (will not all pass — infra panics; document baseline)
docker --version || podman --version              # need a container runtime
git checkout -b sprint-02-jepsen-reactivation
```

## TDD work items

### W2.1: Negative test confirming the orchestrator-wires-mock bug

**RED.** Test `orchestrator_uses_mock_when_real_cluster_provided`: build a `ClusterInfo` with real Docker addresses; call `run_single_combination(..., Some(&cluster), ...)`; assert the workload's executed query went to `MockCqlSession`, not the real cluster. This test passes today (proving the bug).

**GREEN.** Once the bug exists as a tracked test, we know the post-fix test must invert it: same test but assert the workload's query went to the real cluster. Prefix with `#[should_fail_after_fix]` (custom attribute or a marker comment).

**REFACTOR.** None.

### W2.2: Fix the orchestrator wiring (the one-line bug)

**RED.** Test `orchestrator_uses_real_cluster_when_provided`: same setup as W2.1 but asserts the query reached the real cluster. Currently fails because of the discarded `_cluster: Option<&ClusterInfo>` argument at `ferrosa-jepsen/src/orchestrator.rs:203`.

**GREEN.** Use `cluster_opt` to build a real CQL session via `cdrs-tokio` (or the driver registry); pass to the workload. Update `run_single_combination` signature so the cluster is required when not in `--dry-run`.

**REFACTOR.** Delete or feature-gate `MockCqlSession` so it is only used in unit tests, not in the end-to-end path.

### W2.3: `/admin/membership-snapshot` HTTP endpoint

**RED.** Test `admin_membership_snapshot_returns_all_four_maps`: spin up a single ferrosa node; GET `/admin/membership-snapshot`; assert the JSON has fields `state_members`, `openraft_membership`, `node_map`, `peer_manager_peers`. Initially fails — endpoint doesn't exist.

**GREEN.** Add the endpoint to the existing admin HTTP server (search for `/admin/` registration in ferrosa). Returns the four maps as JSON.

**REFACTOR.** Cache the snapshot for 100 ms so repeated polling doesn't lock-storm.

### W2.4: Six structural invariants as post-run check

**RED.** Test `membership_snapshot_invariants_hold_on_clean_3_node_cluster`: bring up the existing 3-node Jepsen cluster; collect snapshots from every node via the W2.3 endpoint; run the six structural invariants from `specs/raft-invariants.md` §B (I-06, I-07, I-08, I-09, I-10, I-13). All pass.

**GREEN.** Add a `MembershipChecker` module in `ferrosa-jepsen/src/checker/membership.rs`. Implements one check function per invariant. Hooked into `UnifiedChecker::check_all`.

**REFACTOR.** Each check returns a structured `InvariantViolation` instead of a panic; `UnifiedChecker` aggregates.

### W2.5: Negative test — Sprint 1 revert produces invariant failure

**RED.** Test `membership_invariants_fail_on_silent_drop_revert`: artificially revert the silent-drop fix from `controller/membership.rs:428-431` (or apply equivalent code that swallows ForwardToLeader); run smoke tier; assert `MembershipChecker` reports a violation of I-06.

**GREEN.** No new code — the test exists to lock the regression. Add a `#[ignore = "regression-only; run manually with FERROSA_REGRESSION_TEST=1"]` marker only if the revert can't be cleanly automated.

**REFACTOR.** Document the regression-test pattern in the sprint's README.

### W2.6: Topology nemesis `add-node-via-follower`

**RED.** Test `add_node_via_follower_succeeds`: 3-node cluster running register workload; nemesis spins up node4 with `FERROSA_SEED=node3` (forces dial to a non-leader); assert post-run that node4 is in every node's membership snapshot.

**GREEN.** Implement `chaos/add_node.rs`: spawns a new container with computed env vars (host_id, seed pointing at a chosen-by-name follower). Uses the existing docker-compose machinery.

**REFACTOR.** Pull common container-spawn machinery into `chaos/topology.rs`.

### W2.7: Topology nemesis `decommission-leader`

**RED.** Test `decommission_leader_completes`: 3-node cluster; nemesis identifies the current leader (via `current_leader` metric) and runs `ferrosa-ctl decommission` on it; assert post-run cluster has 2 nodes and a new leader.

**GREEN.** Implement in `chaos/decommission.rs`. Uses `ferrosa-ctl` over SSH or via the admin HTTP API.

**REFACTOR.** None.

### W2.8: Topology nemesis `random-startup-order`

**RED.** Test `random_startup_order_does_not_break_formation`: rerun cluster bring-up 10 times with shuffled `depends_on` ordering; assert formation succeeds every time, leader varies.

**GREEN.** Implement in `chaos/startup_order.rs`. Uses Docker compose's `--scale` and explicit start ordering.

**REFACTOR.** Replace the asymmetric seed config in `tests/docker/jepsen-cluster.yml` with full mesh seeds (every node lists every other) — a precondition for random order.

### W2.9: Workload `membership-churn`

**RED.** Test `membership_churn_workload_completes`: 3-node baseline cluster; workload adds + removes nodes every 5s for 30s; assert post-run no membership invariant violations.

**GREEN.** Implement in `workload/membership_churn.rs`. Each operation is `MembershipChanger::add_voter` or `remove_voter` (Sprint 1 dependency for full effect; for now use raw `ferrosa-ctl` commands).

**REFACTOR.** Parameterize churn rate and direction (add-only, remove-only, mixed).

### W2.10: Workload `forward-probe`

**RED.** Test `forward_probe_succeeds_against_followers`: identify a non-leader; issue `MembershipChanger::update_metadata` against it; assert it succeeds (returns Ok, applied on leader).

**GREEN.** Implement in `workload/forward_probe.rs`. Specifically targets the bug class fixed in Sprint 1 W1.5.

**REFACTOR.** None.

### W2.11: Workload `late-join-flood`

**RED.** Test `late_join_flood_converges`: base 3-node cluster; burst-add 5 fresh nodes simultaneously; after 30 s assert all 5 are in every node's snapshot.

**GREEN.** Implement in `workload/late_join_flood.rs`. Uses the W2.6 nemesis to spawn 5 concurrent late joiners.

**REFACTOR.** None.

### W2.12: Wire Knossos via the existing Clojure subproject

**RED.** Test `knossos_runs_register_history`: run a register workload, write history to a file, invoke Knossos via `lein run`, assert exit 0 with linearizable verdict.

**GREEN.** Replace the `knossos: None` stub at `ferrosa-jepsen/src/checker/mod.rs:278-282` with a wrapper that shells to `lein run` in `ferrosa-jepsen/jepsen/`. Parses Knossos output.

**REFACTOR.** Add a feature flag `--no-knossos` for fast iterations.

### W2.13: Lift `--exclude ferrosa-jepsen` from CI for Tier::Smoke

**RED.** Add a CI job `jepsen-smoke` that runs `cargo test -p ferrosa-jepsen --features=tier-smoke` with `FERROSA_TEST_CONTAINERS=1`. Initially fails because of the exclusion at `.github/workflows/ci.yml:53`.

**GREEN.** Lift the exclusion *only* for the new `jepsen-smoke` job (other CI jobs keep the exclusion). Add the necessary docker-compose bring-up to the workflow.

**REFACTOR.** Cache docker-compose images between runs.

### W2.14: Symmetric seed config

**RED.** Test `cluster_yml_seed_list_is_symmetric`: parses `tests/docker/jepsen-cluster.yml` and asserts every node has every other node in its `FERROSA_SEED` env var.

**GREEN.** Edit the YAML to symmetric. Verify W2.8 still passes.

**REFACTOR.** None.

## Acceptance criteria (sprint-level)

- [ ] `orchestrator_uses_real_cluster_when_provided` (W2.2)
- [ ] `admin_membership_snapshot_returns_all_four_maps` (W2.3)
- [ ] `membership_snapshot_invariants_hold_on_clean_3_node_cluster` (W2.4)
- [ ] `membership_invariants_fail_on_silent_drop_revert` (W2.5) — regression-only
- [ ] `add_node_via_follower_succeeds`, `decommission_leader_completes`, `random_startup_order_does_not_break_formation` (W2.6, W2.7, W2.8)
- [ ] `membership_churn_workload_completes`, `forward_probe_succeeds_against_followers`, `late_join_flood_converges` (W2.9, W2.10, W2.11)
- [ ] `knossos_runs_register_history` (W2.12)
- [ ] CI job `jepsen-smoke` runs on every PR; passes (W2.13)
- [ ] `cluster_yml_seed_list_is_symmetric` (W2.14)
- [ ] **Cross-sprint regression test**: reverting Sprint 1 W1.1 makes the Sprint 2 smoke run fail. Demonstrates the harness catches the bug class.

## Parallelization within Sprint 2

Two tracks:
- **Track A (orchestrator + invariants)**: W2.1–W2.5, W2.12, W2.13.
- **Track B (nemeses + workloads)**: W2.6–W2.11, W2.14.

A 2-engineer team finishes Sprint 2 in ~1 week.

## Risks

- **R1 — Docker bring-up flaky in CI**: known infra risk. Mitigation: retry policy in CI workflow; budget 6 minutes per smoke run.
- **R2 — Knossos JVM is slow to start**: ~5–10s per invocation. Mitigation: run Knossos only at end of run, not per workload op.
- **R3 — Symmetric seed config exposes formation races**: this is the *point* — if races exist, they must be fixed (overlap with Sprint 1 W1.16). Coordinate with Sprint 1.

## Completion checklist

- [ ] Branch + PR exist.
- [ ] CI `jepsen-smoke` job green on PR.
- [ ] Coordinator file updated to Sprint 2 = completed.

## Kickoff prompt for an agent

> You are executing Sprint 2 of the Ferrosa Raft Correctness Program. Spec at `/home/bkearns/src/ferrosa-suite/raft-correctness/specs/in-process/sprint-02-jepsen-reactivation.md`.
>
> Goal: convert `ferrosa-jepsen` from theatre to a working harness that runs on every PR. Implement W2.1–W2.14 in TDD order. Each item: RED → GREEN → REFACTOR; commit per item; CI green throughout.
>
> Companion reading: ADR-016 (verification stack), Agent C's audit (summary in `specs/raft-correctness-plan.md` §F3), `jepsen-e2e-test-plan.md`, `specs/raft-invariants.md` §B.
>
> Constraints: strict TDD; no `#[ignore]`; container runtime required (`FERROSA_TEST_CONTAINERS=1`). Use a worktree at `/home/bkearns/src/ferrosa-suite/sprint-02/` off main.
>
> If Knossos or Docker infra blocks W2.12 or W2.13, document in the sprint file and proceed with the rest; do not block on infra you can't unblock.
