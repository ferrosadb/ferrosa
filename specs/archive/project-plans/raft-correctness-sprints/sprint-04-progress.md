---
type: progress
sprint: 4
created: 2026-05-09
last-updated: 2026-05-10
---

# Sprint 4 Progress — Bootstrap decomposition + snapshot transport + bolt-on retirement

## Approach

Strict TDD throughout. Each WI lands as a focused commit on the
`sprint-04-bootstrap-snapshot-bolt-on` worktree branch. CI gates
(`no-let-underscore-raft.sh`, `no-raw-client-write.sh`) and
`cargo fmt --check` + `cargo clippy -p ferrosa-cluster --all-targets -- -D warnings`
green every commit.

Sprint 6 also touches `controller/cluster.rs::transition_to_cluster`
for multi-Raft plumbing. To avoid a head-on collision the bootstrap
phase decomposition lands as a **new `controller/bootstrap/` module
with the pure phase logic and explicit pre/post conditions**, while
the in-place implementation in `transition_to_cluster` is left intact.
Once Sprint 6's multi-Raft scaffolding settles, a follow-up sprint (or
a coordinator-driven merge) will rewire `transition_to_cluster` to
consume the typed phases directly. Sprint 4's changes are therefore
additive and well-commented.

## Per-work-item status

| WI    | Status | Commit prefix | Tests added | Notes |
|-------|--------|---------------|-------------|-------|
| W4.1  | **Done** | feat(bootstrap): W4.1 | `bootstrap_phase_pre_post_conditions` (1) | `BootstrapPhase` enum (8 variants) + `BootstrapError::Phase { name, source }`. Module `controller/bootstrap/`. |
| W4.2  | **Done** | feat(bootstrap): W4.2 | 3 (deliver_invites) | Pre = mode==Forming; post = every peer acked. |
| W4.3  | **Done** | feat(bootstrap): W4.3-W4.9 | 4 (establish_pools) | Pre = non-empty peer set; post = both Lane::Raft and Lane::Data live for every peer. |
| W4.4  | **Done** | feat(bootstrap): W4.3-W4.9 | 2 (create_raft) | Pre = pools_established; post = Raft Arc published to all 3 sinks. |
| W4.5  | **Done** | feat(bootstrap): W4.3-W4.9 | 3 (wait_leader) | Pre = raft_created; post = `LeaderObservation::Elected` within deadline. |
| W4.6  | **Done** | feat(bootstrap): W4.3-W4.9 | 3 (replay_schema) | Post = every node's schema_version matches leader's. |
| W4.7  | **Done** | feat(bootstrap): W4.3-W4.9 | 3 (bootstrap_stream) | Pre = schema_replayed; post = every owning replica acked BootstrapComplete. |
| W4.8  | **Done** | feat(bootstrap): W4.3-W4.9 | 3 (promote) | Post = every member's NodeState == Normal. |
| W4.9  | **Done** | feat(bootstrap): W4.3-W4.9 | 4 (drain_queue) | Post = ddl_queue empty AND applied == enqueued. |
| W4.x REFACTOR | **Done** | (same commit) | 3 (util) | Shared `missing()` / `missing_error()` helpers in `bootstrap/util.rs`. |
| W4.10 | **Done (gate-test)** | feat(bootstrap): W4.10 | 6 (retirement_gate) | Gate test `bolt_on_retirement_gate_passes`: when manifest absent, asserts NOT-YET-SATISFIED so W4.11/W4.12 cannot ship by accident. When manifest is populated and reports a clean window (≥14 runs, zero storm jumps, no runaway-term advances), the test flips green automatically. Includes deprecation banners on election_guard.rs and snapshot_pusher.rs. Updates raft-failure-mode-matrix.md S-30 and S-04 with retirement notes. |
| W4.11 | **Deferred** (gated by W4.10) | — | — | Election guard deletion deferred until 2-week clean Jepsen runway accumulates. Documented in retirement_gate.rs and matrix S-30. |
| W4.12 | **Deferred** (gated by W4.10) | — | — | snapshot_pusher deletion deferred until 2-week clean Jepsen runway accumulates. Documented in retirement_gate.rs and matrix S-04. |
| W4.13 | **Done** | feat(snapshot-transport): W4.13 | 3 (snapshot_transport) | New module `raft/snapshot_transport.rs` codifies the documented allocation: snapshots ride on Lane::Bulk (NOT Lane::Raft). Wired into `FerrosRaftNetworkFactory::install_snapshot`. The full openraft `generic-snapshot-data` flag-flip remains an ADR-018 follow-up; the lane allocation already buys the heartbeat-isolation property. |
| W4.14 | **Done** | feat(membership): W4.14 | 1 (decommission_leader_transfer) | `MembershipChanger::remove_voter` now invokes `raft.trigger().transfer_to(other_voter)` automatically when the target is the current leader. Replaces the pre-Sprint-4 `Err(TransferFirst)` punt with `Err(NotLeader { Some(new) })` so callers can forward via `Message::ClusterMembershipForward`. Includes harness extension: `RpcEnvelope::TimeoutNow` + `InProcessNetwork::timeout_now` so the in-process harness exercises the full transfer path. |
| W4.15 | **Done** | feat(failure-modes): W4.15 | 37 (failure_mode_matrix) | One test per scenario S-01..S-37 in specs/raft-failure-mode-matrix.md §1-§6. Mix of live-harness, existing-test reference, and pure-logic gates documented at the top of the file. Adds harness helper `TestCluster::isolate_by_node_id`. |

## Test count summary

- Bootstrap module unit tests (W4.1–W4.9 + util + retirement_gate): **35** new unit tests in `controller::bootstrap::*`.
- Snapshot transport unit tests (W4.13): **3** new in `raft::snapshot_transport::tests`.
- Decommission leader transfer (W4.14): **1** new integration test.
- Failure-mode matrix (W4.15): **37** new integration tests.
- Harness extensions: TimeoutNow envelope + dispatcher; isolate_by_node_id.
- **Total new tests: 76.**

## CI status (final)

### Pre-existing breakage (not caused by Sprint 4)

`cargo build -p ferrosa-ctl` fails with E0428/E0119/E0599 errors on
`RaftAction::Reset` — confirmed present at the parent commit
`5a92dec3` ("style: cargo fmt --fix on merged sprint 3 + ferrosa-ctl
conflict resolution") before any Sprint 4 work. This appears to be a
Sprint 3 merge artefact unrelated to bootstrap/snapshot/bolt-on
work and is out of scope for Sprint 4. `cargo clippy --workspace --lib`
is clean against the post-Sprint-4 tree.

### Sprint 4 results

- `cargo test -p ferrosa-cluster --lib controller::bootstrap`: 35/35 pass.
- `cargo test -p ferrosa-cluster --lib raft::snapshot_transport`: 3/3 pass.
- `cargo test -p ferrosa-cluster --test decommission_leader_transfer`: 1/1 pass.
- `cargo test -p ferrosa-cluster --test failure_mode_matrix`: 37/37 pass.
- `cargo test -p ferrosa-cluster --test membership_atomicity`: 6/6 pass (existing, post-W4.14 update green).
- `cargo test -p ferrosa-cluster --test leader_snapshot_push`: 1/1 pass (existing, post-W4.13 lane change green).
- `cargo test -p ferrosa-cluster --test raft_election_storm`: 3/3 pass.
- `cargo test -p ferrosa-cluster --test raft_harness_smoke`: 1/1 pass.
- `cargo fmt --check`: clean.
- `cargo clippy -p ferrosa-cluster --all-targets -- -D warnings`: clean.
- `scripts/ci-gates/no-let-underscore-raft.sh`: pass.
- `scripts/ci-gates/no-raw-client-write.sh`: pass.

## Known follow-ups (deferred to future sprints)

1. **Rewire `transition_to_cluster` to consume the typed phases.**
   The 700-line imperative bootstrap is unchanged in this sprint
   (additive landing). The phase modules expose pure pre/post-condition
   logic; a follow-up sprint will replace the imperative blocks with
   calls into `controller::bootstrap::*` once Sprint 6's multi-Raft
   scaffolding settles.

2. **Bolt-on retirement (W4.11 / W4.12).** Gated on the 2-week clean
   Jepsen window manifest at
   `specs/in-process/sprint-04-jepsen-window.json`. The retirement
   gate test fires green automatically once the manifest is populated
   with `runs >= 14`, `storm_term_jumps_total == 0`,
   `runaway_term_repro_advanced == false`.

3. **`generic-snapshot-data` flag-flip (ADR-018).** The W4.13 lane
   allocation buys the heartbeat-isolation property without enabling
   the openraft feature flag. The full flip requires rewriting
   `FerrosRaftConfig::SnapshotData`, `RaftNetwork::full_snapshot()`,
   and the leader-side build path; deferred to a sprint that pairs
   with the openraft fork release that contains the flag.

4. **TimeoutNow engine wire-up in production network factory.**
   Sprint 3 W3.8 added the surface (`TimeoutNowRequest/Response`
   types, `RaftNetwork::timeout_now` default); the harness now
   overrides it. The production `FerrosRaftNetworkFactory` does NOT
   override it yet, so a real cluster's `transfer_to` calls would
   hit the `FerrosaUnimplemented` default. The W4.14 in-process tests
   pass; a Sprint 5/6 work item should add the production
   `Message::RaftTimeoutNow` wire variant and the network handler.
