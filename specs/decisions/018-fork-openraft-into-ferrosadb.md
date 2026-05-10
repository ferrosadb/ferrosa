# ADR-018: Fork openraft into `ferrosadb/openraft`

> Date: 2026-05-09
> Status: Proposed
> Companion to: ADR-012 (PreVote, CheckQuorum, Leadership Transfer)

## Context

We already depend on a fork of openraft 0.9 at `github.com/ferrosadb/openraft.git` branch `fix/separate-replication-timeout` (per `Cargo.toml:24`). This ADR formalizes that fork as a long-lived ferrosa-owned project, sets contribution policy, and adds the patches from ADR-012.

## Decision

### Repository layout

- Upstream: `github.com/databendlabs/openraft` (open-source, BSD-3).
- Fork: `github.com/ferrosadb/openraft`.
- Default branch: `ferrosadb/main` — tracks upstream `main` with our patches rebased on top.
- Release branches: `ferrosadb/0.9` — current production; `ferrosadb/1.0` once upstream cuts a 1.0.

### Patch set

| Patch | Origin | Upstream-mergeable? |
|---|---|---|
| `fix/separate-replication-timeout` | existing | Yes — file PR. |
| `correctness/checkquorum` | ADR-012 | Yes — file PR. |
| `correctness/leadership-transfer` | ADR-012 | Yes — file PR. |
| `correctness/prevote` | ADR-012 | No — author has declined. Carry as fork-only. |

We file each as a separate PR upstream. Best-effort. If accepted, drop from our fork.

### Modeled after Scylla's Cassandra fork

Scylla's relationship with Apache Cassandra is the model: maintain compatibility, contribute fixes upstream when alignment exists, carry vendor-specific behavior in the fork. We do the same with openraft.

### Cargo.toml policy

```toml
[workspace.dependencies]
openraft = { git = "https://github.com/ferrosadb/openraft.git", branch = "ferrosadb/0.9", features = ["serde", "storage-v2", "loosen-follower-log-revert"] }
```

Branch name updates per release (Sprint 8 evaluates moving to `ferrosadb/1.0` once upstream cuts).

### `loosen-follower-log-revert` audit (Sprint 1 deliverable)

This cargo feature relaxes Raft's log-monotonicity invariant — followers can truncate log entries the leader still considers committed. Acceptable **only** during deliberate disaster-recovery rebootstrap (e.g., wiping a node's raft data dir).

Audit:

1. Trace every code path that could trigger a follower log revert. Document each.
2. Confirm steady-state operation never triggers it. (If it does, that's silent data loss masked by a flag.)
3. Add metric `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` that fires on every revert, with operator-action correlation field.
4. If the metric is non-zero in production telemetry over 30 days without a correlated wipe-and-rejoin operator action, downgrade the feature flag to `cfg(debug_assertions)` only — production builds disable it.

### Snapshot transport — `generic-snapshot-data` enabled in Sprint 4

openraft's `generic-snapshot-data` cargo feature lets the application bypass openraft's chunking entirely. Useful when SM > 1 GiB to avoid heartbeat starvation during InstallSnapshot, and to decouple snapshot transport from `Lane::Raft` so a 100 MB snapshot install does not block heartbeats.

**Plan**: enable `generic-snapshot-data` in Sprint 4 alongside the bootstrap-task decomposition. Implementation:

- Cargo.toml: add `generic-snapshot-data` to features.
- New module `ferrosa-cluster/src/raft/snapshot_transport.rs`. Implements openraft's `RaftNetworkV2::full_snapshot` (or equivalent in 0.9.x) using a dedicated TCP connection per stream. Reuses `PeerManager` for addr resolution but bypasses the lane multiplex.
- New `Lane::Snapshot` (or reuse `Lane::Bulk`) for the dedicated channel. Heartbeats stay on `Lane::Raft`.
- Chunk size: 8 MiB (vs openraft default 3 MiB) — fewer chunks, less per-chunk overhead, fits well inside TCP receive windows on a dedicated connection.
- The `snapshot_pusher` retirement (per ADR-012) and this change are both Sprint 4. Order: `snapshot_pusher` retired first, then `generic-snapshot-data` lands. They are independent.

Rationale for moving this in: ferrosa already routinely produces multi-MB snapshots (schema + 256 tokens per node + index state map), and the bug genome shows snapshot install during sustained AppendEntries traffic correlates with election timeout near-misses. Doing this work now while we are already in the snapshot path (`snapshot_pusher` retirement) avoids a second pass.

## Rationale

We already have a fork; documenting it as policy clarifies expectations for future contributors. Adding ADR-012's patches has no choice — PreVote is rejected upstream — so we either fork or accept the liveness gap.

## Consequences

### Positive

- Clear ownership; no surprise when CI breaks because upstream openraft moved.
- A place to land ferrosa-specific extensions (witnesses if ever needed, see ADR-015).

### Negative

- Maintenance burden: rebase per upstream release. 0.9 is on a slow cadence (~6 months between versions); manageable.
- We are responsible for testing our patches; upstream's CI does not exercise our code paths.

### Neutral

- The fork is open-source (BSD-3); other ferrosa users automatically inherit our patches.

## Open questions

1. **When does ferrosa move to openraft 1.0?** Upstream signals "stable API by 1.0". Sprint 8 evaluates. Default: stay on 0.9 until at least one stable upstream 1.0 release.
2. **Should we open the fork for community contributions?** Yes — same license. Document contribution policy in repo README.

## Acceptance criteria

**Sprint 1 (audit + escape hatch)**:
- `loosen-follower-log-revert` audit complete; every code path documented; runtime metric `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` exposed.
- `ferrosa-ctl raft reset --node N` lands (the operator escape hatch from worktree `ferrosa-raft-fix`).

**Sprint 3 (fork + protocol patches)**:
- Fork `github.com/ferrosadb/openraft` exists with `ferrosadb/0.9` branch.
- ADR-012 patches (PreVote, CheckQuorum, Leadership Transfer) landed in fork; ferrosa Cargo.toml repointed.
- Each patch filed upstream as a PR (PreVote may be rejected; CheckQuorum and Leadership Transfer should land).

**Sprint 4 (transport + bolt-on retirement)**:
- `generic-snapshot-data` cargo feature enabled.
- `ferrosa-cluster/src/raft/snapshot_transport.rs` implements custom transport on `Lane::Snapshot`.
- Test: 100 MB snapshot install during 1000 writes/sec sustained AppendEntries produces no `RAFT_LANE_DELAY_P99` excursions on `Lane::Raft`.
- `election_guard` and `snapshot_pusher` retired (gated on 2-week clean Jepsen window per ADR-012).

## References

- Scylla fork of Apache Cassandra (model).
- openraft README (databendlabs/openraft).
- ADR-012, ADR-015, ADR-016.
