---
type: sprint-progress
status: in-progress
sprint: 2
created: 2026-05-09
---

# Sprint 2 Progress: Jepsen reactivation + structural invariants

Worktree: `/home/bkearns/src/ferrosa-suite/sprint-02-jepsen-reactivation/`
Branch: `sprint-02-jepsen-reactivation` off `feature/raft-gap-close`.
Companion specs: `sprint-02-jepsen-reactivation.md`, `raft-invariants.md` §B, `decisions/016-verification-stack.md`.

## Baseline observations (before changes)

- `cargo build -p ferrosa-jepsen` clean.
- `cargo clippy --all-targets` clean.
- `cargo test -p ferrosa-jepsen --lib`: 175 passing, 8 failing (all infrastructure-gated panics:
  Firecracker, full docker_provision, real CQL session). These match the documented "infra panics"
  baseline.
- Docker available (29.1.3); Podman available (4.9.3).
- Sprint 1 work (`MembershipChanger`) is not yet present in this branch — work items that depend
  on it will use the existing `RaftCommand`/`ferrosa-ctl` surface and adapt once Sprint 1 lands.

## Per-item status

| Item | Title | Status | Notes |
|------|-------|--------|-------|
| W2.1 | Negative test confirming orchestrator-wires-mock bug | | |
| W2.2 | Fix orchestrator wiring | | |
| W2.3 | `/admin/membership-snapshot` endpoint | | |
| W2.4 | Six structural invariants as post-run check | | |
| W2.5 | Cross-sprint regression test (Sprint 1 revert) | | |
| W2.6 | Topology nemesis `add-node-via-follower` | | |
| W2.7 | Topology nemesis `decommission-leader` | | |
| W2.8 | Topology nemesis `random-startup-order` | | |
| W2.9 | Workload `membership-churn` | | |
| W2.10 | Workload `forward-probe` | | |
| W2.11 | Workload `late-join-flood` | | |
| W2.12 | Wire Knossos via Clojure subproject | | |
| W2.13 | Lift `--exclude ferrosa-jepsen` from CI for Tier::Smoke | | |
| W2.14 | Symmetric seed config | | |

## Final branch state

(updated at sprint close)
