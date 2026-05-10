---
type: sprint-progress
status: in-progress
priority: P0
created: 2026-05-09
sprint: 1
session: 2026-05-09 agent-execution-1
---

# Sprint 1 Progress Log

This document records the state of Sprint 1 (Membership Atomicity +
Bug-Class Amnesty) at the end of the first execution session.

## Branch

`sprint-01-membership-atomicity` off `feature/raft-gap-close`.

10 commits landed; all green: `cargo test -p ferrosa-cluster --lib`
(671 passed), `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo fmt --check`.

## Items completed (10 of 21)

| Item | SHA | Test status |
|---|---|---|
| **W1.8**  CI gate `let _ =` in Raft state machine | `d7f5f36e` | shell gate at `scripts/ci-gates/no-let-underscore-raft.sh` returns OK |
| **W1.10** `SledLogStore::reset` operator escape hatch | `3220d60f` | 3 tests pass: `reset_clears_log_and_meta_and_counts_what_was_removed`, `reset_on_empty_store_is_a_noop_with_zero_counts`, `reset_fails_if_sled_lock_is_held` |
| **W1.11** `ferrosa-ctl raft reset --node N` command | `95989861` | 4 tests pass: `ferrosa_ctl_raft_reset_recovers_runaway_term_node`, `ferrosa_ctl_raft_reset_dry_run_does_not_mutate`, `subcommand_raft_reset_parses`, `subcommand_raft_reset_dry_run_flag`. **Caveat**: the integration variant (3-node + term inflation) is deferred — the multi-node Raft test harness for that lands in Sprint 3. |
| **W1.12** `loosen-follower-log-revert` audit | `cb728c4d` | Audit doc at `specs/in-process/sprint-01-loosen-follower-log-revert-audit.md`. **Caveat**: metric `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` deferred — requires patching the openraft fork (Sprint 3 / ADR-018 deliverable). Log-based alert documented as interim. |
| **W1.15** `parking_lot::Mutex` no-poison | `58e4c984` | Test `controller_mutex_does_not_propagate_poison` passes. The migration was already in place; the test pins the contract. |
| **W1.16** Concurrent mode transitions serialize | `6e90eca2` | Test `concurrent_mode_transitions_serialize` passes. Existing `transition_guard` is the serialization mechanism; the test pins it so a guard-less rewrite would regress. |
| **W1.18** Pair role assigned by connection direction | `ec2765b4` | Test `pair_role_assigned_by_connection_direction` passes. **Caveat**: the deprecated `PairRole::elect` call site in `PairNode::new` survives — removing it requires changing PairNode's construction signature to accept `is_inbound: bool`, which touches 11+ test/integration callers; tracked separately. |
| **W1.19a** Append fires callback on error (refactor) | `3071c622` | `append` now routes both Ok and Err through `callback.log_io_completed`. **Caveat**: the canonical test name `append_invokes_callback_on_error` requires constructing an `openraft::storage::LogFlushed`, whose `new` is `pub(crate)` inside openraft — not callable from this crate. The Err path is exercised by code inspection; `append_inner` returning `Result` is unit-testable. Tracked under the openraft-fork patch set. |
| **W1.19b** `save_committed` flushes meta tree | `3071c622` | Test `save_committed_flushes` passes (drop+reopen round-trip). |
| **W1.20** `SledLogStore::purge` runs in `spawn_blocking` | `06dda87b` | Test `purge_does_not_block_heartbeats` passes (current_thread runtime + sibling tick task accumulates ticks during the purge). |
| **W1.21** `recover_membership` fails loud on joint mismatch | `d2f39b52` | Tests `recover_membership_fails_loud_on_lost_joint_config` and `recover_membership_succeeds_on_matching_single_config` pass. New `RecoveryError` enum + `try_recover_membership_from_topology_state` fallible variant. The pre-existing infallible `recover_membership_from_topology_state` is kept for back-compat; migrating callers to the fallible variant is follow-up. |

## Items not started — blocked or deferred

### Blocked on multi-node openraft test harness (Sprint 3 dependency)

These items require an in-process 3-node openraft cluster harness that
does not yet exist in `ferrosa-cluster/tests/`. The existing test
helpers (`tests/cluster_formation.rs`'s `TestClusterNode`) are heavy
real-network setups; they are unsuited for unit-style atomicity tests.
Building the harness is itself a sprint-scale piece of work.

- **W1.1**  `MembershipChanger::add_voter` happy path. Requires
  multi-node Raft fixture to assert all-four-maps coherence.
- **W1.2**  `add_voter` idempotence.
- **W1.3**  `add_voter` retry on `InProgress`.
- **W1.4**  `remove_voter` clears all four maps.
- **W1.5**  `update_metadata` propagates addr.
- **W1.6**  `RaftOp::ApproveNode` replicates to followers.
- **W1.13** `Message::ClusterMembershipForward` generalization (depends
  on `MembershipOp` enum from W1.5).
- **W1.14** Hazard P0-1 — block/queue DDL during Forming. The DDL
  queue + replay logic is in place (`ddl_path::DdlPath::Forming`,
  `controller/cluster.rs:1220-1233`). The W1.14 spec calls out a race
  where ops sent *during* drain are dropped; verifying the fix needs
  multi-node test infrastructure.

### Blocked on apply-path rewrite (W1.7)

- **W1.7**  `apply_command` propagates errors. The current
  `state_machine.rs::apply_command` has 35 sites where Schema /
  Engine / system_writer errors are logged with `tracing::error!` but
  not returned. Refactoring each requires either:
  - threading an error accumulator through the entire match (35 arms,
    high risk of regressing the existing 671 tests), or
  - introducing a typed `ApplyError` enum and converting each site
    site-by-site, which is the right shape long-term.

  Both shapes are sprint-scale. The named test
  `apply_command_propagates_engine_register_failure` requires either
  trait-mocking the engine or stubbing — also non-trivial. Deferred to
  the next session.

### Blocked on production-quality W1.5 prerequisite

- **W1.9**  CI gate `no_raw_client_write_outside_membership` plus
  migration of 11 call sites.

  The gate script is implemented at
  `scripts/ci-gates/no-raw-client-write.sh` (uncommitted). It correctly
  identifies the 11 violating sites:
    - `ferrosa-cluster/src/ddl_path.rs:465` (1)
    - `ferrosa-cluster/src/rebalance.rs:308` (1)
    - `ferrosa-cluster/src/controller/membership.rs` (7)
    - `ferrosa-cluster/src/controller/cluster.rs:409, 412` (2 register_node)

  Migrating these requires the `MembershipChanger` API (W1.1–W1.5)
  which is itself blocked. Once the API lands, these 11 call sites
  migrate one-by-one with their existing tests as the regression
  net.

  **Decision**: do NOT commit the gate script yet — committing it
  while it's still failing would either (a) need temporary `# allow`
  comments embedded in the audit list (a code smell) or (b) break CI.
  Hold the script as a working artifact until W1.1–W1.5 land.

### Forming → Pair fallback timeout (W1.17)

- **W1.17** Wire `formation_timeout_secs` to the Forming state's
  deadline. Today, `formation_timeout_secs` is consumed only by the
  promotion-timeout branch in `transition_to_cluster`
  (`controller/cluster.rs:1535-1538`). The Forming → Pair fallback
  exists at the leader-election timeout boundary
  (`controller/cluster.rs:1598-1614`) but is hard-coded to ~30s and
  not driven by `formation_timeout_secs`. Wiring this end-to-end
  needs a timer in `transition_to_forming` independent of the
  election poll, plus a unit test that does not require a
  live-network multi-node fixture. Achievable in 1-2 hours of
  focused work; deferred to next session.

### Log entry version prefix (W1.19c)

- **W1.19c** 1-byte log-entry version prefix. The current
  `serialize_entry` / `deserialize_entry` in `log_store.rs` already
  has a fallback path (current → legacy) but a corrupt entry that
  happens to bincode-decode as a legacy variant produces silent
  reinterpretation. Adding a version prefix is the right fix; the
  migration path needs a careful staging plan to avoid bricking
  running clusters (entries written before the prefix lands have no
  byte to discriminate). Deferred to a follow-up sprint with explicit
  migration design.

## Final branch state

```
$ git log --oneline 940e0356..HEAD
95989861 feat(membership): W1.11 — ferrosa-ctl raft reset operator escape hatch
cb728c4d docs(membership): W1.12 — loosen-follower-log-revert audit (first pass)
6e90eca2 feat(membership): W1.16 — concurrent mode transitions serialize via transition_guard
d2f39b52 feat(membership): W1.21 — recover_membership_from_topology_state fails loud on joint mismatch
d7f5f36e feat(membership): W1.8 — CI gate forbids `let _ =` in Raft state machine
3071c622 feat(membership): W1.19a/b — append fires callback on error, save_committed fsyncs
06dda87b feat(membership): W1.20 — SledLogStore::purge runs in spawn_blocking
58e4c984 feat(membership): W1.15 — assert parking_lot mutexes do not propagate poison
ec2765b4 feat(membership): W1.18 — pair role assigned by connection direction
3220d60f feat(membership): W1.10 — SledLogStore::reset operator escape hatch
```

## Suggested next session priorities

1. **Build the openraft multi-node test harness** (1-2 days). Without
   this, W1.1–W1.6, W1.13, and W1.14 cannot land. The harness is a
   wrapper around `openraft::testing` plus ferrosa's
   `RaftStateMachine` and `FerrosRaftNetworkFactory`. Once it exists,
   the Sprint 1 membership tests are straightforward.

2. **Land `MembershipChanger` skeleton with W1.1** (2-3 days).
   Implements the 8-step `add_voter` flow with the helper extracted
   per ADR-013. This unblocks W1.2–W1.5 and W1.13.

3. **Migrate the 11 raw `client_write` sites** (1 day, after
   MembershipChanger). Each site has an existing test; commit one
   site at a time. After the last migration, commit the W1.9 CI gate.

4. **W1.7 apply_command rewrite** (1 day, parallel to the above).
   Define `ApplyError`, replace the 35 `tracing::error!` sites with
   typed propagation, surface via `RaftResponse::Error`.

5. **W1.17 Forming → Pair fallback timeout** (2-3 hours). Self-
   contained.

6. **W1.19c log entry version prefix migration plan** (writing only;
   implementation in a follow-up sprint).
