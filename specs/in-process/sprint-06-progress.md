---
type: sprint-progress
status: complete
priority: P1
sprint: 6
wave: 2
created: 2026-05-10
---

# Sprint 6 Progress: Multi-DC scaffolding

> Branch: `sprint-06-multi-dc-scaffolding` (off `feature/raft-gap-close`).
> Spec: `specs/in-process/sprint-06-multi-dc-scaffolding.md`.

## Session 1 — 2026-05-10 — Sprint 6 implementation

Pre-flight: Sprints 1 + 3 already merged. Sprint 4 not yet in flight (no progress
file yet); proceeded additively per orchestrator instructions — every Sprint 6
change kept transition_to_cluster's bootstrap monolith intact so Sprint 4's
decomposition can land without conflict.

### Per-WI status

| WI    | Status    | Commit         | Notes |
|-------|-----------|----------------|-------|
| W6.1  | done      | `47f209ef`     | `RaftGroupId` UUID v5 newtype + frozen namespace; serde transparent over `Uuid`; `default_dc()` shim. |
| W6.2  | done      | `5af74b6a`     | `ModeController` carries `Arc<ArcSwap<HashMap<RaftGroupId, Arc<FerrosRaft>>>>`. `.raft()` is a backward-compat shim (returns the only group; falls through to local-DC for multi-DC). New: `raft_for_dc`, `raft_for_group`, `raft_groups`, `set_raft_for_group`, `set_raft_for_dc`. |
| W6.3  | done      | `e15e42b7`     | `partition_peers_by_dc` filters local-DC voters; `raft_log_dir_for_dc` lays out `<base>/<dc>/`. `controller.record_peer_dc(host, dc)` lets the peer handshake (and tests) publish per-peer DC. Backward-compat: peers with no recorded DC default to local. |
| W6.4  | done      | `9d6dd9ca`     | `MembershipChanger::for_dc(dc, raft, network)`; `dc_name()`, `group_id()` accessors. `MembershipChanger::new(...)` shimmed onto `DEFAULT_DC_NAME`. |
| W6.5  | done      | `b5a4efb5`     | `coordinator::cl_routing::route_for_cl(cl, dc_count)` returns `LocalDcOnly` / `AllDcs` / `NotImplementedCrossDc(ClusterError::NotImplemented)`. New `ClusterError::NotImplemented { feature }` variant. Coordinator integration deferred to Sprint 7 (avoids conflict with Sprint 4 bootstrap decomposition). |
| W6.6  | done      | `1b41f9da`     | `tests/multi_dc_partition.rs` — two independent `TestCluster`s simulate WAN partition; LOCAL_QUORUM holds; cross-DC CLs return `NotImplemented`. |
| W6.7  | done      | `e8f8cc00`     | `ferrosa-ctl cluster bootstrap-dc --dc <name> --seeds <a,b,c>` derives `RaftGroupId` and prints the operator plan. Also fixed a pre-existing botched merge from Sprint 3 (duplicate `RaftAction` and `Raft` variant) that broke `cargo build -p ferrosa-ctl`. |
| W6.8  | done      | `b39086a9`     | `ClusterConfig::per_dc_overrides: BTreeMap<String, PerDcOverride>`. `raft_data_dir_for_dc()` / `rack_for_dc()` resolve effective values. `from_env` parses `FERROSA_RAFT_DATA_DIR_<DC>` / `FERROSA_RACK_<DC>`. `transition_to_cluster` honors the override. |
| W6.9  | done      | `8da4b9bc`     | `ferrosa-jepsen/tests/docker/jepsen-cluster-t3.yml` — 3+3 dual-DC topology with `dc1-net` / `dc2-net` / `wan-net`. `tests/t3_topology.rs` validates structure on every `cargo test`; live bring-up gated on `FERROSA_TEST_CONTAINERS=1` (panics with setup instructions otherwise, per the test policy). |
| —     | done      | `decaa875`     | clippy hygiene: type alias for `partition_peers_by_dc` return type; doc reflow. |

### Acceptance criteria

- [x] `RaftGroupId` exists; tests pass (W6.1).
- [x] Multi-Raft `ModeController` works; backward-compat with single-DC (W6.2).
- [x] DC-aware formation (W6.3).
- [x] Per-DC `MembershipChanger` (W6.4).
- [x] Routing tests pass (W6.5).
- [x] DC-partition nemesis produces clean degraded mode (W6.6).
- [x] `ferrosa-ctl cluster bootstrap-dc` works (W6.7).
- [x] Config plumbing (W6.8).
- [x] T3 topology brings up clean (W6.9 — YAML + structural tests; live bring-up gated).
- [x] Existing single-DC clusters continue working unchanged — verified via the
      685 lib tests and the existing `membership_atomicity` / `cluster_formation`
      integration tests passing untouched.

### Sprint 7 hand-off notes

- Cross-DC consensus (Accord) replaces every `NotImplementedCrossDc(...)` arm
  in `coordinator/cl_routing.rs`. The grep target is
  `ClusterError::NotImplemented { feature }` — every site is at the chokepoint.
- `controller.record_peer_dc(host, dc)` needs to be called by the peer
  handshake (currently only tests call it). Sprint 7 wiring point: the
  `PeerEventListener::on_peer_connected` path or the `HandshakeRequest`
  message in `ferrosa-net`.
- The per-DC log dir layout (`<base>/<dc>/`) is a one-way migration.
  Operators upgrading from single-DC need to either symlink
  `<base>/<existing_dc>/` → existing flat data, or do an offline move.
  Documented as ADR-015 R3.

### CI / hygiene

- `cargo fmt --check` — clean.
- `cargo clippy -p ferrosa-cluster --all-targets -- -D warnings` — clean.
- `cargo clippy -p ferrosa-ctl --all-targets -- -D warnings` — clean.
- `cargo clippy -p ferrosa-jepsen --all-targets -- -D warnings` — clean.
- `cargo test -p ferrosa-cluster --lib` — 685 / 685 pass.
- `cargo test -p ferrosa-cluster --test multi_dc_raft_groups` — 2 / 2.
- `cargo test -p ferrosa-cluster --test multi_dc_formation` — 3 / 3.
- `cargo test -p ferrosa-cluster --test multi_dc_membership_changer` — 2 / 2.
- `cargo test -p ferrosa-cluster --test multi_dc_partition` — 1 / 1.
- `cargo test -p ferrosa-cluster --test membership_atomicity` — 6 / 6 (regression).
- `cargo test -p ferrosa-ctl` — 108 / 108 + 11 / 11 integration.
- `cargo test -p ferrosa-jepsen --test t3_topology` — 4 / 4 structural;
  `t3_topology_brings_up_two_dcs` panics without `FERROSA_TEST_CONTAINERS=1`
  per test policy.

### Pre-existing issues observed (not in scope)

- `accord_nemesis::disk_fail_no_phantom_commits` panics without
  `FERROSA_TEST_CLUSTER_NODES` / `FERROSA_TEST_FIRECRACKER`. Confirmed
  pre-existing (also fails on `5a92dec3`).
- ferrosa-ctl `bin` had duplicate `RaftAction` / `Raft` variants from a
  Sprint 3 botched merge — fixed in W6.7's commit since W6.7 needed
  the binary to compile.

### Final commit count

10 commits on `sprint-06-multi-dc-scaffolding` (off `feature/raft-gap-close`):
W6.1 → W6.9 (one each) plus `decaa875` (clippy hygiene).

