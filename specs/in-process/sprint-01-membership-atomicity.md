---
type: sprint
status: pending
priority: P0
created: 2026-05-09
sprint: 1
wave: 1
---

# Sprint 1: Membership atomicity + bug-class amnesty

> Branch: `sprint-01-membership-atomicity` off `main` of the inner ferrosa repo (`/home/bkearns/src/ferrosa-suite/ferrosa/`).
> Companion to: ADR-013 (Membership Change Protocol), ADR-018 (loosen-follower-log-revert audit).

## Goal

Collapse the four-membership-maps drift bug class — the dominant defect class in the bug genome (P0-21 saga + `fbfc39c8` + 4 sibling silent drops) — into a single transactional API. Land the runaway-term operator escape hatch. Audit `loosen-follower-log-revert`. Resolve the still-open hazards from `hazards-cluster-formation.md` (P0-1, P1-1, P1-3, P1-4, P1-5).

## Hard dependencies

None. Wave 1.

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git status                                    # working tree clean
git checkout main && git pull
cargo test --workspace --lib                  # baseline green
cargo clippy --all-targets -- -D warnings     # baseline clean
cargo fmt --check                             # baseline clean
git checkout -b sprint-01-membership-atomicity
```

If any baseline check fails, abort and report — do not proceed onto a broken main.

## TDD work items

Each work item: write the failing test first, run it, watch it fail for the right reason, then write minimal code to pass, then refactor with tests staying green. Commit at the end of each item.

### W1.1: `MembershipChanger` skeleton + `add_voter` happy path

**RED.**
- New test file `ferrosa-cluster/tests/membership_atomicity.rs`.
- Test `add_voter_updates_all_four_maps`: spin up an in-process 3-node openraft test cluster (use `openraft::testing::TestCluster` or build one from the existing `controller/tests.rs` helpers); call `MembershipChanger::add_voter(node4_uuid, node4_addr)`; assert on every node:
  - `controller.state.members.contains(&node4_uuid)`
  - `raft.metrics().borrow().membership_config.voter_ids().contains(&node4_node_id)`
  - `network_factory.node_map.read().contains_key(&node4_node_id)`
  - `peer_manager.peers.read().contains_key(&node4_uuid)`
- Run: `cargo test -p ferrosa-cluster --test membership_atomicity add_voter_updates_all_four_maps` → expect compile error (`MembershipChanger` doesn't exist).

**GREEN.**
- Create `ferrosa-cluster/src/membership/mod.rs` with `pub struct MembershipChanger { raft, network_factory, peer_manager, config }` and `pub async fn add_voter(...)`.
- Implement the 8 steps from ADR-013 § "Module: `ferrosa-cluster/src/membership/`". For W1.1 the steps may be inlined; W1.5 will refactor.
- Forwarding when not leader: reuse the `raft_forward.rs` pattern from branch `fix/membership-forward-to-leader` (cherry-pick that work into this branch first if not already merged).

**REFACTOR.**
- Extract the wait-for-apply-barrier into a private helper.

**Verify.** `cargo test --workspace --lib` green. Clippy green.

### W1.2: `add_voter` idempotence

**RED.** Test `add_voter_idempotent`: call `add_voter(node4)` twice in succession; second call returns `Ok(())` and does not produce duplicate state. Currently fails because step 5 (`change_membership(AddVoters)`) errors on already-member.

**GREEN.** Make each step a NoOp on already-applied state per ADR-013's idempotence contract. `change_membership` already returns Ok for already-member targets in openraft 0.9; verify and document.

**REFACTOR.** Extract idempotence checks into shared helpers.

### W1.3: `add_voter` retry on `InProgress`

**RED.** Test `add_voter_concurrent_serializes`: two `tokio::spawn` tasks each call `add_voter` for distinct new nodes simultaneously. Both must eventually succeed.

**GREEN.** In `MembershipChanger`, wrap `change_membership` calls in retry-on-`ChangeMembershipError::InProgress` with exponential backoff (10ms, 30ms, 100ms, 300ms, 1s, 3s, 10s, fail).

**REFACTOR.** Pull the retry into a `retry_change_membership` helper.

### W1.4: `remove_voter` removes from all four maps

**RED.** Test `remove_voter_clears_all_four_maps`: add then remove a node; assert all four maps no longer contain it.

**GREEN.** Implement `remove_voter` per ADR-013. For the leader-self-decommission case, return `MembershipError::TransferFirst` until Sprint 3 lands `Leadership Transfer` — then this branch becomes "transfer leadership then proceed."

**REFACTOR.** Both `add_voter` and `remove_voter` share the join-wait pattern; extract.

### W1.5: `update_metadata` (the today's-bug case)

**RED.** Test `update_metadata_propagates_addr`: a follower calls `MembershipChanger::update_metadata(host_id, new_addr=...)` while the local node is not the leader. The proposal forwards to the leader; on apply, every node sees the new addr in `state.members`.

**GREEN.** This is the Sprint-1 production-quality replacement for the membership-forwarding patch in `raft_forward.rs` from the `fix/membership-forward-to-leader` branch. Move the forwarder into `membership/forward.rs`; rename `Message::ClusterRaftForward` to `Message::ClusterMembershipForward` to signal the broader scope (W1.13 generalizes to other ops).

**REFACTOR.** `add_voter`, `remove_voter`, `update_metadata` all share the "issue Raft proposal, classify result, forward if non-leader, retry if InProgress" pattern. Pull into a private `propose` method.

### W1.6: `RaftOp::ApproveNode` is replicated

**RED.** Test `approve_node_replicates_to_followers`: in a 3-node cluster, call `MembershipChanger::approve_node(host_id)` on the leader; assert every follower's `state.approved_nodes.contains(&host_id)`.

**GREEN.** Today `controller.approve_node` mutates only the controller-local cache (`controller/membership.rs:42-44`). Change it to propose `RaftOp::ApproveNode` via `MembershipChanger`. The local cache becomes a derived view populated by apply.

**REFACTOR.** Remove the now-redundant local-cache write site.

### W1.7: `apply_command` returns errors instead of swallowing

**RED.** Test `apply_command_propagates_engine_register_failure`: stub the engine to return Err on `register_table`; propose `RaftOp::CreateTable`; assert `client_write` returns `RaftResponse::Error(_)` (or the error reaches the caller).

**GREEN.** Rewrite `apply_command` in `raft/state_machine.rs` to bubble all sub-errors. Replace every `let _ = engine.register_table(...)` with proper match. `RaftResponse::Error(_)` is no longer dead code.

**REFACTOR.** Define a typed `ApplyError` enum to replace `RaftResponse::Error(String)`.

### W1.8: CI gate — no `let _ =` in raft state machine

**RED.** Add CI step (Bash gate): `! grep -rn "let _ = " ferrosa-cluster/src/raft/state_machine.rs ferrosa-cluster/src/raft/handlers.rs ferrosa-cluster/src/raft/network.rs`. Initially fails because at least one survivor.

**GREEN.** Audit the survivors; replace each with a typed propagation OR document with `#[allow(let_underscore_drop, reason = "...")]` per case.

**Verify.** Grep returns zero. CI gate passes.

### W1.9: CI gate — no raw `client_write` outside `membership/` module

**RED.** Add CI step: `! grep -rn "raft\.client_write\|raft\.add_learner\|raft\.change_membership\|network_factory\.register_node" ferrosa-cluster/src/ | grep -v "ferrosa-cluster/src/membership/"`. Initially fails (Sprint 1 W1.1–W1.6 may have left some).

**GREEN.** Migrate every survivor to `MembershipChanger`. Sites: `controller/cluster.rs:1110-1175` (seed bootstrap), `controller/cluster_rejoin.rs`, `controller/membership.rs`, `ddl_path.rs::ClusterDdlForwardHandler`. Test that each migration preserves behaviour via existing tests.

**REFACTOR.** This may surface API gaps in `MembershipChanger`; add what's needed (e.g., `approve_and_add_voter` if the rejoin path needs combined semantics).

### W1.10: `SledLogStore::reset` operator escape hatch

**RED.** Cherry-pick the test `reset_clears_log_and_meta_and_counts_what_was_removed` from worktree `ferrosa-raft-fix` (branch `fix/raft-reset-and-stuck-detect`) into this branch. Run; expect compile error (`SledLogStore::reset` doesn't exist on main).

**GREEN.** Cherry-pick the implementation. Verify the existing tests pass.

**REFACTOR.** None — the worktree's code already passed review.

### W1.11: `ferrosa-ctl raft reset --node N` command

**RED.** Test `ferrosa_ctl_raft_reset_recovers_runaway_term_node`: integration test that (a) brings up a 3-node cluster, (b) artificially inflates one node's persisted term to T18000, (c) runs `ferrosa-ctl raft reset --node N3`, (d) asserts on next start N3 rejoins as a fresh Learner and converges.

**GREEN.** Add the subcommand to `ferrosa-ctl`. Wire to `SledLogStore::reset`.

**REFACTOR.** Add a `--dry-run` flag.

### W1.12: `loosen-follower-log-revert` audit + metric

**RED.** Test `follower_log_revert_metric_fires_on_revert`: simulate a wiped-and-rejoined follower; assert `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` increments by 1.

**GREEN.** Add the metric. Audit every code path that could trigger a follower log revert (use openraft source + existing tests). Document findings in `specs/in-process/sprint-01-loosen-follower-log-revert-audit.md`. If the audit finds a steady-state trigger, that is a P0 bug — file separately and resolve before sprint completes.

**REFACTOR.** Add an alarm rule to monitoring config (file in `config/`) that fires on any non-zero increment without a correlated wipe-and-rejoin operator action.

### W1.13: `Message::ClusterMembershipForward` generalization

**RED.** Test `cluster_membership_forward_carries_typed_op`: a `MembershipOp::AddVoter`, `MembershipOp::RemoveVoter`, etc. round-trips through the forwarding wire format.

**GREEN.** In `ferrosa-net/src/message.rs`, replace `Message::ClusterRaftForward(Bytes)` with `Message::ClusterMembershipForward(Bytes)` carrying a serialized `MembershipOp` (enum with explicit variants instead of an opaque `RaftCommand`). Update encode/decode and `MsgType` accordingly.

**REFACTOR.** Document the wire format in `ferrosa-net/src/message.rs` doc comment.

### W1.14: Hazard P0-1 — block or queue DDL during Forming

**RED.** Test `ddl_during_forming_queues_and_replays`: enter Forming state; issue `CREATE TABLE`; before Raft init completes, transition to Cluster; assert the table exists on every node post-leader-election.

**GREEN.** Per `hazards-cluster-formation.md` P0-1: queue DDL during Forming via the existing `ddl_queue_rx` mpsc; replay through Raft on leader election (the current implementation drops anything queued *during* drain — fix that).

**REFACTOR.** None.

### W1.15: Hazard P1-1 — `parking_lot::Mutex` migration

**RED.** Test `controller_mutex_does_not_propagate_poison`: panic in a critical section while holding the controller's `connected_peers` mutex; assert subsequent locks succeed (via `parking_lot::Mutex` no-poison semantics).

**GREEN.** Replace 17 instances of `std::sync::Mutex` in `controller/mod.rs` with `parking_lot::Mutex`. List in `hazards-cluster-formation.md` P1-1.

**REFACTOR.** None.

### W1.16: Hazard P1-3 — mode CAS

**RED.** Test `concurrent_mode_transitions_serialize`: two `tokio::spawn` callers attempt mode transitions on the same controller simultaneously; only one succeeds.

**GREEN.** Replace the load-then-store mode-change pattern with `ArcSwap::compare_and_swap` (or the equivalent). Hold a transition guard across check-and-transition.

**REFACTOR.** None.

### W1.17: Hazard P1-4 — Forming → Pair fallback timeout

**RED.** Test `forming_falls_back_to_pair_on_timeout`: enter Forming with `formation_timeout_secs=1`; do not transition to Cluster; after 1 s, assert mode is Pair.

**GREEN.** Wire the existing `formation_timeout_secs` config to the Forming state's deadline. On expiry, transition back to Pair. Restore DDL path to Direct.

**REFACTOR.** None.

### W1.18: Hazard P1-5 — connection-direction roles

**RED.** Test `pair_role_assigned_by_connection_direction`: node A connects outbound to node B; assert A is Secondary, B is Primary, regardless of UUID order.

**GREEN.** Replace `PairRole::elect` (UUID comparison) with the connection-direction signal already present via `InboundPeerCallback::on_inbound_peer`.

**REFACTOR.** Remove the now-dead `PairRole::elect`.

### W1.19: `SledLogStore` resilience

**RED.** Three tests:
- `append_invokes_callback_on_error`: failing sled write produces `callback.log_io_completed(Err(_))`.
- `save_committed_flushes`: simulate crash between save_committed and the next vote; assert recovery sees the committed marker.
- `legacy_log_decode_unambiguous`: a corrupt entry that happens to bincode-decode as a legacy variant produces an explicit error, not silent reinterpretation.

**GREEN.**
- `append`: invoke callback on error.
- `save_committed`: add explicit flush.
- Add a 1-byte log-entry version prefix; legacy = `0x00`, current = `0x01`. Migration: on read, if first byte `< 0x20` (legacy variant index range), treat as version 0 (no prefix); otherwise expect version byte. Document in source.

**REFACTOR.** Centralize the version-byte handling in `serialize_entry`/`deserialize_entry`.

### W1.20: `purge` runs in `spawn_blocking`

**RED.** Test `purge_does_not_block_heartbeats`: under sustained AppendEntries traffic, trigger a purge of 1000 entries; assert `RAFT_LANE_DELAY_P99` on `Lane::Raft` stays under `heartbeat_interval / 2`.

**GREEN.** Move the purge body in `SledLogStore::purge` into a `spawn_blocking` block, mirroring `append`.

**REFACTOR.** None.

### W1.21: `recover_membership_from_topology_state` fails loud on joint mismatch

**RED.** Test `recover_membership_fails_loud_on_lost_joint_config`: artificially construct a state where the actual last committed Membership log entry was a joint config but the snapshot is older; recovery's synthesized `Membership::new(vec![voters], None)` does not match the joint config; assert recovery returns `RecoveryError::JointConfigLost(_)` rather than silently downgrading.

**GREEN.** Modify `recover_membership_from_topology_state` to require matching against an actual log entry. Document via I-19 in `specs/raft-invariants.md`.

**REFACTOR.** None.

## Acceptance criteria (sprint-level)

Tests that must pass on `main` after Sprint 1 PR merges:

- [ ] `add_voter_updates_all_four_maps` (W1.1)
- [ ] `add_voter_idempotent` (W1.2)
- [ ] `add_voter_concurrent_serializes` (W1.3)
- [ ] `remove_voter_clears_all_four_maps` (W1.4)
- [ ] `update_metadata_propagates_addr` (W1.5)
- [ ] `approve_node_replicates_to_followers` (W1.6)
- [ ] `apply_command_propagates_engine_register_failure` (W1.7)
- [ ] CI gate `no_let_underscore_in_raft_state_machine` (W1.8)
- [ ] CI gate `no_raw_client_write_outside_membership` (W1.9)
- [ ] `reset_clears_log_and_meta_and_counts_what_was_removed` (W1.10)
- [ ] `ferrosa_ctl_raft_reset_recovers_runaway_term_node` (W1.11)
- [ ] `follower_log_revert_metric_fires_on_revert` (W1.12)
- [ ] `cluster_membership_forward_carries_typed_op` (W1.13)
- [ ] `ddl_during_forming_queues_and_replays` (W1.14)
- [ ] `controller_mutex_does_not_propagate_poison` (W1.15)
- [ ] `concurrent_mode_transitions_serialize` (W1.16)
- [ ] `forming_falls_back_to_pair_on_timeout` (W1.17)
- [ ] `pair_role_assigned_by_connection_direction` (W1.18)
- [ ] `append_invokes_callback_on_error`, `save_committed_flushes`, `legacy_log_decode_unambiguous` (W1.19)
- [ ] `purge_does_not_block_heartbeats` (W1.20)
- [ ] `recover_membership_fails_loud_on_lost_joint_config` (W1.21)
- [ ] `loosen-follower-log-revert` audit doc landed at `specs/in-process/sprint-01-loosen-follower-log-revert-audit.md`
- [ ] CI green on `cargo test --workspace --lib`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`.

## Parallelization within Sprint 1

Three tracks can proceed in parallel after W1.1 lands:

- **Track A (Membership API)**: W1.2, W1.3, W1.4, W1.5, W1.6 — same module, serialize within track.
- **Track B (Apply path + CI gates)**: W1.7, W1.8, W1.9, W1.13, W1.21 — independent of track A.
- **Track C (Operator + audit + hazards)**: W1.10, W1.11, W1.12, W1.14, W1.15, W1.16, W1.17, W1.18, W1.19, W1.20 — touches different files; mostly independent.

A 3-engineer team can land Sprint 1 in ~2 weeks. A single engineer in ~4–5 weeks.

## Risks

- **R1 — `MembershipChanger` API surface mismatches a use case**: discovered during W1.9 migration. Mitigation: design review at end of W1.6.
- **R2 — `RaftOp::ApproveNode` change breaks existing approval flow**: existing `auto_join=false` clusters may rely on the local-cache-only behaviour. Mitigation: feature-flag the change behind `ferrosa.approval.replicated=true`; default true for new deployments, false for existing.
- **R3 — `loosen-follower-log-revert` audit finds a steady-state trigger**: this is a latent silent-data-loss bug. Mitigation: stop sprint, file P0, fix before resuming.
- **R4 — Refactoring 25+ call sites of raw `client_write` introduces regressions**: Mitigation: each site migrated under its own existing test; no test deletions.

## Completion checklist

- [ ] Branch `sprint-01-membership-atomicity` exists.
- [ ] All work items W1.1–W1.21 committed (each commit green).
- [ ] PR opened to `main`.
- [ ] CI green on PR.
- [ ] All acceptance criteria checkboxes ticked.
- [ ] `specs/in-process/sprint-00-coordinator.md` updated with Sprint 1 status `completed`.
- [ ] PR merged.

## Kickoff prompt for an agent

> You are executing Sprint 1 of the Ferrosa Raft Correctness Program. The full sprint plan is at `/home/bkearns/src/ferrosa-suite/raft-correctness/specs/in-process/sprint-01-membership-atomicity.md`. Read it first; it is your authoritative spec.
>
> Your task: execute work items W1.1 through W1.21 in TDD order (RED → GREEN → REFACTOR per item). Each work item produces a green commit. Do not move to the next item until the current one's tests pass and clippy/fmt are clean. The sprint ends when all acceptance-criteria tests pass on a PR to `main`.
>
> Companion reading (load only as needed):
> - `specs/raft-correctness-plan.md` (umbrella).
> - `specs/decisions/013-membership-change-protocol.md` (the API spec).
> - `specs/decisions/018-fork-openraft-into-ferrosadb.md` (loosen-follower-log-revert audit policy).
> - `specs/raft-invariants.md` (invariants this sprint enforces).
> - `specs/hazards-cluster-formation.md` (P0-1, P1-1, P1-3, P1-4, P1-5 are in-scope).
>
> Constraints:
> - Strict TDD. Never write production code without a failing test.
> - No `#[ignore]` anywhere.
> - No `let _ = ` outside test code (CI gate enforces in `raft/`).
> - Each commit ends green: `cargo test --workspace --lib`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`.
> - Branch off `main` of `/home/bkearns/src/ferrosa-suite/ferrosa/`. Use a worktree at `/home/bkearns/src/ferrosa-suite/sprint-01/` (create with `git worktree add ../sprint-01 -b sprint-01-membership-atomicity main`).
>
> If you discover a P0 bug during the audit (W1.12), stop and report rather than continue; that is a separate ticket.
>
> Report progress after every 4 work items; report immediately on a hard failure that you cannot resolve via the spec.
