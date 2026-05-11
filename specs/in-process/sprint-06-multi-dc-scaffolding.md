---
type: sprint
status: pending
priority: P1
created: 2026-05-09
sprint: 6
wave: 2
---

# Sprint 6: Multi-DC scaffolding (Raft-per-DC)

> Branch: `sprint-06-multi-dc-scaffolding`.
> Companion to: ADR-015 (multi-DC), ADR-013 (membership), ADR-014 (learners).

## Goal

Wire the topology that lets DC1 and DC2 run independent per-DC Raft groups for metadata. No cross-DC consensus yet — cross-DC writes return `NotImplemented` until Sprint 7. T3 topology (3+3 dual-DC) brings up two healthy Raft groups that survive `dc-partition` cleanly.

## Hard dependencies

- **Sprint 1 merged**: `MembershipChanger` is the only path to membership change.
- **Sprint 3 merged**: PreVote + CheckQuorum + Leadership Transfer.

## Pre-flight checks

```sh
cd /home/bkearns/src/ferrosa-suite/ferrosa
git checkout main && git pull
git log --grep "Sprint 1\|Sprint 3" --oneline | head
cargo test --workspace --lib && cargo clippy && cargo fmt --check
git checkout -b sprint-06-multi-dc-scaffolding
```

## TDD work items

### W6.1: `RaftGroupId` type

**RED.** Test `raft_group_id_is_uuid_per_dc`: assert `RaftGroupId` is a `Uuid` newtype; equality, hash, serde work.

**GREEN.** Define `pub struct RaftGroupId(pub Uuid);` in `ferrosa-cluster/src/raft/group_id.rs`. Per-DC groups derive `RaftGroupId` deterministically from the DC name (e.g., UUID v5 with a fixed namespace).

**REFACTOR.** None.

### W6.2: `ModeController` carries `Map<RaftGroupId, Arc<FerrosRaft>>`

**RED.** Test `controller_holds_one_raft_per_dc`: bring up a 3+3 dual-DC simulated cluster (using Sprint 5 sim or in-process test harness); assert `controller.raft_groups.len() == 2`.

**GREEN.** Refactor `controller.raft_instance: Arc<ArcSwap<Option<Arc<FerrosRaft>>>>` into `controller.raft_groups: Arc<ArcSwap<HashMap<RaftGroupId, Arc<FerrosRaft>>>>`. Update every accessor. Backward compat: if only one group, `controller.raft()` returns it; else `controller.raft_for_dc(dc_name)`.

**REFACTOR.** Pull `RaftGroupHandle` accessor methods into a trait.

### W6.3: Cluster formation detects DC and routes to per-DC Raft

**RED.** Test `cluster_formation_creates_separate_raft_per_dc`: 6 nodes, 3 in `dc1` and 3 in `dc2` (per `FERROSA_DC` env var); assert each DC's nodes form their own Raft group.

**GREEN.** Update `controller/cluster.rs` `transition_to_cluster` to read `self.config.data_center`, group peers by DC, instantiate one `FerrosRaft` per DC. Each DC's Raft has its own log dir under `raft_data_dir/<dc-name>/`.

**REFACTOR.** None.

### W6.4: Per-DC `MembershipChanger`

**RED.** Test `membership_changer_scoped_to_dc`: `MembershipChanger::new(dc_name)` returns a changer that operates on that DC's Raft group only.

**GREEN.** `MembershipChanger` (Sprint 1) gains a `RaftGroupId` parameter. Calls `controller.raft_for_dc(dc_name)` instead of `controller.raft()`.

**REFACTOR.** Backward compat for single-DC deployments: default to "default" DC group.

### W6.5: Read/write routing by CL

**RED.** Three tests:
- `local_quorum_routes_within_dc`: write at LOCAL_QUORUM hits only local-DC voters.
- `quorum_returns_not_implemented_cross_dc`: write at QUORUM in dual-DC returns `NotImplemented` (Sprint 7 implements).
- `each_quorum_returns_not_implemented`: same.

**GREEN.** Update `coordinator/write.rs` and `coordinator/read.rs` to consult `RaftGroupId` per partition. Cross-DC fan-out path stubbed with `NotImplemented`.

**REFACTOR.** None.

### W6.6: `dc-partition` nemesis produces clean per-DC degraded mode

**RED.** Test `dc_partition_does_not_cause_global_outage`: Sprint 5 sim test; spawn 3+3 cluster; partition WAN; assert each DC retains LOCAL_QUORUM availability; assert no cross-DC writes succeed.

**GREEN.** With per-DC Raft and Sprint 6 scaffolding, this should already pass. Debug if it doesn't.

**REFACTOR.** None.

### W6.7: `ferrosa-ctl cluster bootstrap-dc` operator command

**RED.** Test `ferrosa_ctl_bootstrap_dc_creates_raft_group`: `ferrosa-ctl cluster bootstrap-dc --dc dc3 --seeds node3a:7000,node3b:7000,node3c:7000` creates a third Raft group; existing DC groups continue running.

**GREEN.** Add the command. Calls into `controller.bootstrap_dc(...)`.

**REFACTOR.** None.

### W6.8: Configuration plumbing

**RED.** Test `cluster_config_per_dc`: `FERROSA_DC=dc1`, `FERROSA_RAFT_DATA_DIR_dc1=...`, etc. — each DC has independent config.

**GREEN.** Update `ClusterConfig` to optionally carry per-DC overrides. Default: single DC named `"default"`.

**REFACTOR.** None.

### W6.9: T3 topology (3+3 dual-DC) bring-up

**RED.** Test `t3_topology_brings_up_two_dcs`: integration test using Docker compose with two networks and 3 nodes each. Assert both DCs reach Cluster mode within 30s.

**GREEN.** Add `tests/docker/jepsen-cluster-t3.yml` with two `network`s (DC1 and DC2 + WAN bridge). Adjust ferrosa-jepsen cluster builder to support multi-network.

**REFACTOR.** None.

## Acceptance criteria

- [ ] `RaftGroupId` exists; tests pass (W6.1).
- [ ] Multi-Raft `ModeController` works; backward-compat with single-DC (W6.2).
- [ ] DC-aware formation (W6.3).
- [ ] Per-DC `MembershipChanger` (W6.4).
- [ ] Routing tests pass (W6.5).
- [ ] DC-partition nemesis produces clean degraded mode (W6.6).
- [ ] `ferrosa-ctl cluster bootstrap-dc` works (W6.7).
- [ ] Config plumbing (W6.8).
- [ ] T3 topology brings up clean (W6.9).
- [ ] Existing single-DC clusters continue working unchanged (regression).

## Parallelization within Sprint 6

- **Track A (Type system)**: W6.1, W6.2, W6.4, W6.8 — serial.
- **Track B (Routing)**: W6.5 — depends on A.
- **Track C (Formation + ops)**: W6.3, W6.7, W6.9 — depends on A.
- **Track D (Tests)**: W6.6 — depends on B and C.

A 2-engineer team finishes in ~1.5 weeks.

## Risks

- **R1 — Refactor of `raft_instance` to `raft_groups` ripples through 30+ call sites**: mitigation: backward-compat shim in W6.2 lets the migration land in pieces.
- **R2 — Existing single-DC tests fail under multi-Raft API**: mitigation: backward-compat ensures default `"default"` DC behaves identically to current single-DC.
- **R3 — Per-DC log dir layout breaks recovery**: existing data goes to `raft_data_dir/`; new layout to `raft_data_dir/<dc>/`. Mitigation: migration script or symlink at upgrade time.

## Completion checklist

- [ ] Branch + PR.
- [ ] CI green.
- [ ] Single-DC and dual-DC tests both pass.

## Kickoff prompt for an agent

> Sprint 6. Spec at `specs/in-process/sprint-06-multi-dc-scaffolding.md`. Hard dependencies: Sprints 1 + 3 merged.
>
> Build the multi-Raft scaffolding. Cross-DC consensus is *not* in scope — Sprint 7 does that.
>
> Companion reading: ADR-015, ADR-013, ADR-014 (learners interact with multi-DC routing).
>
> Worktree at `/home/bkearns/src/ferrosa-suite/sprint-06/`. Strict TDD. Maintain backward-compat with single-DC deployments throughout.
