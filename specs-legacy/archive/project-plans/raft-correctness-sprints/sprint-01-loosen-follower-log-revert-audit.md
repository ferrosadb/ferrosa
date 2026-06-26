---
type: audit
status: in-progress
priority: P0
created: 2026-05-09
sprint: 1
work_item: W1.12
---

# Sprint 1 W1.12: `loosen-follower-log-revert` Audit

> Status: **First-pass audit complete.** No steady-state trigger
> identified. Metric instrumentation (`RAFT_FOLLOWER_LOG_REVERTED_TOTAL`)
> is blocked on the openraft fork (ADR-018 Sprint 3 deliverable) since
> the revert detection is internal to `replication::mod::validate_matching`
> at `openraft-0.9.24/src/replication/mod.rs:549-566` and is not surfaced
> to the application.

## Background

Cargo feature `loosen-follower-log-revert` (enabled in `Cargo.toml`)
relaxes Raft's log-monotonicity invariant in
`Replication::validate_matching` so that a follower's matching log id
can decrease — i.e., the follower's accepted log can shrink. In strict
Raft this is a bug; the feature exists to support deliberate disaster-
recovery rebootstrap where an operator wipes a node's persisted state
and the follower re-replicates from the leader.

Behavior with the flag:
- If `matching > new_matching`: log a warning, allow.
- Without the flag: `debug_assert!` panics in dev/test, no-op in release.

Per ADR-018 § "`loosen-follower-log-revert` audit (Sprint 1
deliverable)":
1. Trace every code path that could trigger a follower log revert.
2. Confirm steady-state operation never triggers it.
3. Add metric `RAFT_FOLLOWER_LOG_REVERTED_TOTAL` that fires on every
   revert.
4. If the metric is non-zero in production telemetry over 30 days
   without a correlated wipe-and-rejoin operator action, downgrade the
   feature flag to `cfg(debug_assertions)` only.

## Trigger paths in openraft

`validate_matching` is called from `ReplicationCore::handle_response`
when the follower replies with the index of the last log entry it now
accepts. The response shape is `ReplicationResult::Matching(matching)`.

Audit of all call sites of `validate_matching` in openraft 0.9.24:

| Location | Call shape | Reachable from steady-state? |
|---|---|---|
| `replication/mod.rs:549` (Replication state machine) | `validate_matching(new_matching)` after follower reports its last log id | Only on transition where the follower's log was truncated below the leader's previously-known matching point. |

The trigger condition for `matching > new_matching` is precisely:

> The follower's response now reports a smaller last-accepted index than
> what the leader previously recorded.

In a healthy Raft this is impossible: a follower never moves its commit
or matching index backwards under any append/truncate semantics. The
exhaustive set of ways for `new_matching < matching` to be observed:

1. **Operator-initiated wipe**: the follower's persistent log was
   reset (e.g., `SledLogStore::reset` from W1.10, or a fresh
   provisioned disk).
2. **Storage corruption**: the on-disk log was partially truncated by
   an unclean shutdown or filesystem failure, and the follower's
   `LogState` reports a smaller `last_log_id` after recovery.
3. **Snapshot install with smaller install LogId**: `InstallSnapshot`
   with a snapshot whose `meta.last_log_id < self.matching`. This
   should never happen in correct openraft because the leader only
   sends snapshots that strictly extend the known state, but a fork-
   level bug could produce it.
4. **Bug in `RaftLogStorage` impl** (e.g., our `SledLogStore`): a
   `LogState` query that under-reports `last_log_id` (e.g., a
   partial-recovery race in `recover_from_purge_point`).

Of these, #1 is intentional and correlated with operator action
(`ferrosa-ctl raft reset`, W1.11). #2-4 are bugs.

## Steady-state assessment

**No steady-state trigger identified.** Specifically:

- `SledLogStore::append` never truncates beyond the new entries' index
  range; it only inserts forward (audited at
  `ferrosa-cluster/src/raft/log_store.rs:633-660`).
- `SledLogStore::truncate` removes a tail beginning at `log_id.index`,
  which is the standard Raft truncate-divergent-suffix path. After
  truncate, `get_log_state` reports a smaller `last_log_id`, but only
  for indices the leader had not yet committed to that follower — so
  `matching` (the *committed* matching point) does not move backwards.
- `SledLogStore::purge` removes a head, which advances
  `last_purged_log_id` but never reduces `last_log_id`.
- `SledLogStore::reset` (W1.10) DOES reduce `last_log_id` to None — but
  it is only called by the operator escape hatch and the node must be
  stopped first, so openraft's replication loop is not running and
  cannot observe the revert until the node restarts as a fresh learner.

**Remaining concerns:**
- `recover_from_purge_point` and the snapshot-restore path could in
  principle synthesize a smaller `last_log_id` if the on-disk state and
  in-memory state disagree. This is exactly the class of bug W1.21
  (`recover_membership_fails_loud_on_lost_joint_config`) addresses for
  membership; an analogous fail-loud check for `last_applied` would be
  useful follow-up work.
- The combination of `loosen-follower-log-revert` + a buggy
  `RaftLogStorage` implementation could mask a real corruption as a
  warning log line. Without metric instrumentation, we cannot detect
  this in production telemetry.

## Metric (deferred)

`RAFT_FOLLOWER_LOG_REVERTED_TOTAL` requires patching openraft's
`validate_matching` to call out to a metrics hook. The fork branch
`ferrosadb/0.9` already exists (per ADR-018 § "Cargo.toml policy"); the
patch is a one-line `metrics::counter!(...).increment(1)` inside the
`if self.matching > matching` arm.

This is **deferred to Sprint 3** (ADR-018 fork formalization) per the
sprint dependency graph: Sprint 1 cannot land openraft fork patches
without the fork policy being in place. The audit conclusion above
makes the deferral safe — no steady-state trigger has been identified,
so we are not flying blind on a known issue, only on hypothetical
ones.

In the interim, the warning log line emitted by openraft's
`validate_matching` when revert happens is grep-able from production
logs:

```
follower log is reverted from .* to .*; with 'loosen-follower-log-revert' enabled, this is allowed
```

A log-based alert can be wired up immediately as a temporary substitute
for the metric. Suggested alert rule (Loki / Grafana / Prometheus
log-pipeline):

```promql
sum(rate({app="ferrosa"} |= "follower log is reverted" [5m])) > 0
```

If this alert fires in any 30-day window without a correlated
`ferrosa-ctl raft reset` operator-action audit log entry, that is a P0:
the silent-data-loss class is active. File a separate ticket and wipe
the affected node per W1.10/W1.11.

## Conclusion

- **Audit step 1 (trace paths)**: complete.
- **Audit step 2 (steady-state confirmation)**: complete — no
  steady-state trigger.
- **Audit step 3 (metric)**: deferred to Sprint 3 fork patches; log-
  based alert is the interim observable.
- **Audit step 4 (downgrade flag if metric stays zero 30 days)**: not
  yet applicable; revisit after Sprint 3 metric lands.

The flag stays enabled in production builds for now. Operator actions
that legitimately require revert (W1.10, W1.11) are documented and
correlatable.
