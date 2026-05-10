---
type: sprint-progress
status: completed-with-partials
sprint: 2
created: 2026-05-09
---

# Sprint 2 Progress: Jepsen reactivation + structural invariants

Worktree: `/home/bkearns/src/ferrosa-suite/sprint-02-jepsen-reactivation/`
Branch: `sprint-02-jepsen-reactivation` off `feature/raft-gap-close`.
Companion specs: `sprint-02-jepsen-reactivation.md`, `raft-invariants.md` §B,
`decisions/016-verification-stack.md`.

## Baseline observations (before changes)

- `cargo build -p ferrosa-jepsen` clean.
- `cargo clippy --all-targets` clean.
- `cargo test -p ferrosa-jepsen --lib`: 175 passing, 8 failing (all infrastructure-gated panics:
  Firecracker, full docker_provision, real CQL session). These match the documented
  "infra panics" baseline.
- Docker available (29.1.3); Podman available (4.9.3).
- Sprint 1 work (`MembershipChanger`) is not yet present in this branch — work items that
  depend on it adapt with the existing `RaftCommand`/`ferrosa-ctl` surface and will gain
  full power once Sprint 1 lands.

## Per-item status

| Item   | Title                                                     | Status     | Notes |
|--------|-----------------------------------------------------------|------------|-------|
| W2.1   | Negative test confirming orchestrator-wires-mock bug      | done       | Folded into W2.2 — added `orchestrator_uses_mock_when_no_cluster_provided` as the `Mock` arm of the resolution helper instead of a separate "bug confirmation" test that would be deleted. The fix path is what's locked. |
| W2.2   | Fix orchestrator wiring                                   | done       | `SessionSource::Real(addrs)` / `SessionSource::Mock` enum + `resolve_session_source(cluster)`. `run_single_combination` now builds `Box<dyn CqlSession>` from the helper. |
| W2.3   | `/admin/membership-snapshot` endpoint                     | done (partial wiring) | Endpoint mounted at `/admin/membership-snapshot`, axum integration test passes. `state_members` is projected from the local token ring; `node_map` aliases `openraft_voters` (network_factory's registry isn't publicly exposed yet — Sprint 4 hardens that). Cross-snapshot drift is still detectable because every reporter resolves on its own state machine. |
| W2.4   | Six structural invariants as post-run check               | done       | New `checker/membership.rs` module: `MembershipSnapshot` (wire shape) + `check_membership_invariants()` covering I-06, I-07, I-08, I-09, I-10, I-13. Wired into `UnifiedChecker::check_all` via `with_membership_snapshots()` builder. One happy path + one violation per invariant tested. |
| W2.5   | Cross-sprint regression test (Sprint 1 revert)            | done       | `membership_invariants_fail_on_silent_drop_revert`: synthesizes the four-maps drift a silent-drop revert would produce; checks that I-06 fires. Cross-snapshot helper handles voter-set drift independently. Test is unit-level so it works **before** Sprint 1 lands and remains valid after. |
| W2.6   | Topology nemesis `add-node-via-follower`                  | done (logic) | `chaos/topology.rs::AddNodeViaFollower` with `pick_follower_seed` helper. Live container path panics without `FERROSA_TEST_CONTAINERS=1`. Pure-function test pins follower selection. |
| W2.7   | Topology nemesis `decommission-leader`                    | done (logic) | `DecommissionLeader` with `parse_leader_from_snapshot` helper that uses `current_leader` field if present, else falls back to first voter. Errors on empty voter list / invalid JSON. |
| W2.8   | Topology nemesis `random-startup-order`                   | done (logic) | `RandomStartupOrder::plan_startup_order(services, rng)` Fisher–Yates shuffle. Property test: 10 RNG seeds produce ≥ 2 distinct first-boot nodes; every order is a permutation. |
| W2.9   | Workload `membership-churn`                               | done       | `workload/membership_churn.rs::MembershipChurnWorkload` with `ChurnMode { AddRemove, AddOnly, RemoveOnly }`. Records add/remove ops; invariant verifies count balance. |
| W2.10  | Workload `forward-probe`                                  | done       | `workload/forward_probe.rs`. UPDATE+SELECT pairs against `jepsen.peers`. The `check_invariant` step locks the silent-drop signature (read of older value after committed write fails with descriptive error mentioning "silent-drop"). |
| W2.11  | Workload `late-join-flood`                                | done       | `workload/late_join_flood.rs`. Burst-records 5 join attempts every interval; invariant requires at least one full burst's worth of writes. |
| W2.12  | Wire Knossos via the existing Clojure subproject          | done       | `UnifiedChecker::check_all_with_history_file(history, history_path)` invokes `KnossosChecker` when both `jepsen_dir` and `history_path` are set. Failures surface as `checker-error` anomalies (fail-loud). Two unit tests pin the wiring without requiring `lein`. |
| W2.13  | Lift `--exclude ferrosa-jepsen` from CI for Tier::Smoke   | done       | New `jepsen-smoke` job in `.github/workflows/ci.yml`: pre-pulls images, sets `FERROSA_TEST_CONTAINERS=1`, runs `cargo test -p ferrosa-jepsen --lib` with infra-gated tests skipped, plus the `ci_workflow` integration tests. The `test` job keeps the legacy exclusion so it doesn't try to spin Docker. |
| W2.14  | Symmetric seed config                                     | done       | `tests/docker/jepsen-cluster.yml`: every node now lists every other node in `FERROSA_SEED`. New `parse_seeds_from_compose()` parser + `cluster_yml_seed_list_is_symmetric()` test pins the property. |

## Test count delta

| Suite                              | Before | After | Notes |
|------------------------------------|--------|-------|-------|
| `cargo test -p ferrosa-jepsen --lib` | 175 ok / 8 infra-fail | 221 ok / 8 infra-fail | +46 unit tests across W2.1–W2.14 |
| `cargo test -p ferrosa --bins`       | 222 ok / 0 fail | 223 ok / 0 fail | +1 endpoint integration test |
| `cargo test -p ferrosa-jepsen --test ci_workflow` | n/a | 2 ok | New |
| Workspace clippy                  | clean | clean | |
| Workspace fmt                     | clean | clean | |

## Items adapted from spec

- **W2.1** was specced as a separate "bug confirmation" test that would be deleted post-fix.
  Folded into the resolution helper's two-arm test (`Real` and `Mock`) so we lock both
  branches permanently. The `Mock` arm pins the no-cluster fallback.
- **W2.3** "state.members" map projects from the **token ring** because the openraft state
  machine isn't publicly accessible from `ModeController`. The token ring is built from
  `state.members` so the projection is faithful for steady-state checks; transient drift
  during apply is not yet visible. `node_map` aliases `openraft_voters` for the same
  reason — Sprint 4 will harden by exposing the network_factory registry. Cross-snapshot
  drift across reporters is still fully detected.
- **W2.6/W2.7/W2.8** ship as library logic with pure-function tests; the live container
  paths panic without `FERROSA_TEST_CONTAINERS=1` per the project's no-`#[ignore]`,
  no-silent-skip policy.
- **W2.12** doesn't run Knossos against a real history (no `lein` needed). The wiring is
  exercised by tests that pin the success path's existence and the failure path's
  fail-loud behavior.

## Known gaps / followups

- **Live container runs not validated.** All Sprint 2 tests are unit/integration-level
  against synthetic snapshots and mock sessions. The Sprint 1 `MembershipChanger` API is
  not yet present in this branch, so end-to-end runs against the real cluster would not
  fully exercise the bug class (today's `RaftCommand::UpdateNodeInfo` path was patched in
  commit `eeb122b1` but the membership-changer atomicity around `add_voter` /
  `remove_voter` is not yet there). When Sprint 1 lands and is merged, run the
  `jepsen-smoke` CI job once with `FERROSA_TEST_CONTAINERS=1` to validate end-to-end.
- **`/admin/membership-snapshot` projections.** `state_members.state` field is a
  lower-cased name (`"normal"` etc.) matching `NodeStateLabel` serde rename; verify the
  jepsen consumer deserializes correctly when wired. `node_map` alias to `openraft_voters`
  is the most material approximation — Sprint 4 should add a `network_factory.peers()`
  accessor.
- **Knossos lein invocation untested live.** No leiningen setup in CI yet; the Clojure
  subproject at `ferrosa-jepsen/jepsen/` exists but `project.clj` may need updates.
  Sprint 4 plus.

## Final branch state

- Branch: `sprint-02-jepsen-reactivation` (off `feature/raft-gap-close`).
- 10 commits sprint2(W2.1–W2.14):
  - `5babc5e1` W2.1/W2.2 — orchestrator wiring fix
  - `c1147a7c` W2.14 — symmetric seed config
  - `fd34e5bd` W2.4 — structural-invariant checker
  - `63067dec` W2.5 — cross-sprint regression test
  - `96e8f5c7` W2.3 — `/admin/membership-snapshot` endpoint
  - `54b04f72` W2.10 — forward-probe workload
  - `423fd19f` W2.6/W2.7/W2.8 — topology nemeses
  - `664442a4` W2.9/W2.11 — membership-churn + late-join-flood workloads
  - `d13e8027` W2.13 — jepsen-smoke CI job
  - `eda77ca0` W2.12 — Knossos wiring
- Not pushed to remote (per kickoff prompt).
- Not merged into `feature/raft-gap-close` (per kickoff prompt).
