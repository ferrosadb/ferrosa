# ADR-016: Verification Stack — TLA+ + Sim + Loom + Jepsen + Runtime

> Date: 2026-05-09
> Status: Proposed
> Companion to: ADR-017 (Sim harness), all other ADRs (each invariant tagged with verification layers)

## Context

Per Agent B's bug genome, 38 Raft fixes in 12 months produced 5 dominant defect classes (formation race, non-leader silent drop, election storm, peer-pool stale, membership drift). Existing verification:

- `cargo test` unit tests (covers some classes).
- `ferrosa-jepsen` (excluded from CI; orchestrator wires `MockCqlSession` instead of real cluster).

The user picked all four verification layers (TLA+, deterministic sim, Loom, beefed-up Jepsen). They overlap deliberately.

## Decision

Five layers, each enforcing a subset of the invariant catalog (`raft-invariants.md`):

| Layer | Catches | Fails to catch | Setup effort |
|---|---|---|---|
| **TLA+ / Apalache** | Protocol design bugs, joint-consensus correctness, PreVote interactions, multi-DC quorum properties. | Implementation bugs, timing, serialization. | 1 sprint (Sprint 5). |
| **Deterministic simulation** | Implementation bugs reproducible by seed, time-travel debugging, full nemesis matrix at 10K+ seeds/min. | Real disk/network bugs; bugs requiring real OS scheduling. | Large sprint to build (Sprint 5); per-workload thereafter. |
| **Loom** | Concurrency bugs in shared-state primitives — mutex-across-await, lane-actor races, RpcClient pending-map. | Anything multi-process. | Days; wraps existing tokio code. Sprint 1. |
| **Jepsen** | End-to-end behaviour against real (containerized) cluster; wire-format issues; cross-driver bugs. | State-space-rare bugs that don't reproduce on this machine. | Reactivation in Sprint 2. |
| **Runtime invariants** | Production drift; catches what upper layers missed. | Nothing without instrumentation. | Constant; inline asserts + Prometheus metrics. |

### Layering rationale (overlap is the point)

The same invariant gets multiple checks. Example: I-06 ("Four-maps agree"):

- **TLA+**: `MembershipInvariant ≜ ∀ n ∈ Nodes : ferrosaMembers[n] = openraftMembers[n] = nodeMap[n] = peerManager[n]`.
- **Sim**: post-step assertion on every membership transition.
- **Loom**: not applicable (not a concurrency invariant).
- **Jepsen**: post-run snapshot diff via `/admin/membership-snapshot` HTTP endpoint.
- **Runtime**: `MEMBERSHIP_DRIFT_NODES` Prometheus gauge; alert if non-zero.

If TLA+ proves the invariant of the protocol but the implementation has a typo, sim catches it. If sim's model differs from reality, Jepsen catches it. If a partial deployment introduces drift past CI, runtime catches it. **Defense in depth.**

### TLA+ specs at `specs/tla/`

- `raft.tla` — leader election with PreVote, AppendEntries with quorum commit, joint-consensus membership, snapshot install. Refines openraft's published TLA+ where possible.
- `multi-dc.tla` — per-DC Raft + Accord cross-DC (Sprint 7).

Apalache is the model checker. Bounded N (≤5 nodes), term ≤ 10, log ≤ 20. Run nightly in a separate workflow; expected runtime <30 min.

### Deterministic simulation harness — see ADR-017

### Loom

Apply to:
- `ferrosa-net/src/lane_actor.rs` — message queue + pending request map under concurrent send.
- `ferrosa-net/src/rpc/client.rs` — pending-request map (today: `DashMap` after the `9fa74ed4` fix; verify with Loom that DashMap usage is correct).
- `ferrosa-cluster/src/raft/election_guard.rs` — state shared between metric-poll and elect-suppress.

CI: `cargo test --features loom -p ferrosa-net -- loom_tests` on every PR. Sprint 1.

### Jepsen reactivation

See `raft-correctness-plan.md` Sprint 2. Three blockers, each one-line/one-config:

1. Fix `orchestrator.rs:203` to use real CQL driver against `cluster.nodes[i].cql_address()`.
2. Lift `--exclude ferrosa-jepsen` from CI.
3. Replace asymmetric seed config in `tests/docker/jepsen-cluster.yml` with symmetric (every node lists every other) + `random-startup-order` test.

Plus the structural invariants and topology nemeses listed in §B/§G of `raft-invariants.md`.

### Runtime invariants

Production deployment exposes Prometheus metrics for every invariant tagged `runtime` in `raft-invariants.md`. Alerts on non-zero counters / gauges trigger pager. Examples:
- `MEMBERSHIP_DRIFT_NODES` (gauge).
- `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` (counter; non-zero requires an operator-action correlation).
- `ELECTION_STORM_TERM_JUMPS_TOTAL` (counter; should be zero post-Sprint-3).
- `RAFT_LANE_DELAY_P99` (gauge; threshold heartbeat_interval / 2).
- `HANDSHAKE_REJECTED_TOTAL{reason}` (counter; reason="cluster-name-mismatch" etc.).

## Rationale

The four-layer stack is the standard for distributed systems correctness in 2026 (TigerBeetle, FoundationDB, CockroachDB all use variants). Layering over each other is what catches the bugs that any single layer misses. Runtime instrumentation closes the loop — production drift detected, fed back into the spec, prompting new TLA+ properties.

## Consequences

### Positive

- Each defect class in the bug genome maps to at least two verification layers.
- New regressions caught at PR time, not deploy time.
- Confidence to refactor (membership module, multi-DC) without fear.

### Negative

- Substantial up-front investment (Sprint 5 is the heavy one; Loom and Jepsen reactivation are smaller).
- TLA+ specs are a maintenance commitment — they must evolve with the implementation.

### Neutral

- Production metrics overhead is small (sub-1% of CPU per the existing `election_guard` polling already in place).

## Acceptance criteria

- All invariants in `raft-invariants.md` are tagged with at least one verification layer; for top-10 invariants, at least two layers.
- Apalache spec checks at bounded sizes pass in nightly.
- Sim harness produces reproducible failures by seed (Sprint 5 acceptance).
- Loom tests pass in CI.
- Jepsen smoke tier runs on every PR; reverting any Sprint 1 fix produces a smoke failure.
- Production runtime metrics exposed via `/metrics`; alerts configured for top-10 invariants.

## References

- TigerBeetle's deterministic-sim approach (referenced in ADR-017).
- FoundationDB's simulation testing.
- Apalache TLA+ checker.
- `specs/raft-invariants.md`.
- `specs/raft-correctness-plan.md` Sprints 1, 2, 5.
