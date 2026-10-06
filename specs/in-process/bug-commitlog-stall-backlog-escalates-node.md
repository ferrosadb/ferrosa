---
title: A continuously-written node aborts on commit-log backlog age, not on a stalled fsync
status: in-process
created: 2026-10-06
updated: 2026-10-06
severity: P0
area: ferrosa/src/supervisor.rs (commit-log sync supervision)
---

# A continuously-written node aborts on commit-log backlog age, not on a stalled fsync

## Symptom

`node3` of the local ferrosa-memory cluster aborted five times on 2026-10-05/06
(19:12, 20:01, 22:54, 00:24, 01:52, 04:25, 04:55:59 — the last exit code `1`), leaving
the cluster at 2/3 and the memory MCP degraded (`lane is reconnecting ... peer_id=33333333`).

```
ERROR ferrosa::supervisor: commit-log fsync stalled; writes are refused and the node
  reports not ready until an fsync completes task="commitlog_sync"
  detail=no commit-log fsync for 4860ms, past the 2000ms stall deadline
  (cause=device-slow(fsync in flight), no failure recorded)
FATAL: supervised task exceeded its restart intensity; aborting so the process
  supervisor restarts the node from the commit log. task=commitlog_sync
  failures_in_period=4 max_restarts=3 period_secs=3600 commit_log_sync=ok
  last_failure=no commit-log fsync for 4860ms, past the 2000ms stall deadline
```

`commit_log_sync=ok` in the FATAL line: the sync thread was not dead and nothing failed.

## What was falsified before the root cause was accepted

The stall text says `cause=device-slow(fsync in flight)`, and the first reading of this
was "the SSD took 4.6 s for a 440-byte write". **That is wrong**; measured and rejected:

- 8 concurrent `F_FULLFSYNC` workers on the live data volume, at host load ~13.5:
  **5947 calls in 14 s (424/s), p50 13 ms, p99 29 ms, max 64 ms, zero calls >= 100 ms.**
  A 4.6 s single fsync is 70x outside any observed tail.
- `cause=` is an *inference*, not a measurement: `SyncHealthSnapshot::stall_cause()`
  returns `DeviceSlow` whenever `attempt_elapsed.is_some()`. A descheduled/starved sync
  thread looks identical. Node1 logged the *other* variant in the same window
  (`cause=no-fsync-attempted(thread starved or not woken)`), so the cause label is
  already known to vary on this host.
- The engine's own runtime-stall detector fired concurrently
  (`runtime scheduling stall ... stall_ms=6144, stalls=9`), and one sync shows
  `write_ms=1602` for 220 bytes with `file_lock_wait_ms≈0` — a starved thread, not a slow device.

Do not re-file this as a disk-latency bug.

## Root cause

`CommitLogSyncSupervisor::record_stalls` (`ferrosa/src/supervisor.rs`) counts one failure
per `stall_deadline` that **`unsynced_for`** has outlived:

```rust
let due = (waited.as_nanos() / health.stall_deadline.as_nanos()) as u64;
```

`unsynced_for` is the age of the **oldest unsynced write**, not the duration of one
in-flight fsync (`ferrosa-storage/src/commitlog/sync.rs`). On a node under continuous
write the backlog never empties, so the episode never "ends": at the default
`stall_deadline = 2000 ms` and `max_restarts = 3`, ~8 s of sustained writes with a
slow-but-progressing sync thread reaches `in_period = 4 > 3` and the node aborts —
while the thread is demonstrably alive, and `commit_log_sync=ok`.

The fix `77dd6eff "a commit-log stall that recovers on its own never aborts the node"`
exempts only deadline #1; deadlines >= 2 still call `record_crash`. That exemption
assumes the episode ends between deadlines, which a backlog behind a busy thread never
does. `attempts_started` / `attempts_completed` (already maintained in production at
`sync.rs:321` / `sync.rs:336`) prove progress and were unused by the supervisor.

## Invariants that must hold

1. **No data loss / no false durable ack.** A write is still refused and `/readyz` still
   reports not ready while the backlog is past the deadline. Unchanged.
2. **A making-progress sync thread is never aborted.** If an fsync completed since the
   previous sample, the node must not escalate, however long the backlog stays behind.
3. **A genuinely stalled or dead thread still escalates.** A backlog whose thread
   completes no fsync (in flight for the whole sample, or no attempt issued, or dead)
   must still reach the intensity and escalate. Do not lose the guard the abort exists for.
4. **The failure metric stays honest.** One episode counts once; it must not silently
   drop to zero (a silent stall is worse than the crash).
5. **Ordering and capacity unchanged.** No change to the commit-log write path, the
   refusal gate, or replay.

## Test list

- [x] `a_busy_sync_thread_that_keeps_completing_a_backlog_past_the_deadline_never_escalates`
      (new) — 12 samples, one completed fsync each, backlog stays past the deadline:
      zero escalations, one counted stall, no `device-slow` cause.
- [ ] `a_stalled_commit_log_sync_counts_one_stall_per_deadline_and_escalates` (existing) —
      must keep failing an *unbroken, no-progress* stall. Re-anchor its unbroken samples to
      no-progress so it still discriminates.
- [ ] `slow_fsync_episodes_that_recover_never_escalate` (existing) — keep.
- [ ] Negative control: break the guard (ignore completed-fsync progress) and confirm the
      new test goes red.
- [ ] `the_stall_cause_is_counted_once_per_episode_not_once_per_deadline` stays green.

## Acceptance criteria

- [ ] RED first: the new test fails on the unmodified `record_stalls`.
- [ ] GREEN: a progress-aware rule in `record_stalls` — count the episode once and never
      `record_crash` while an fsync completed since the last sample.
- [ ] Release-build reasoning recorded for invariant 3 (a truly hung fsync must escalate).
- [ ] `cargo test --bin ferrosa supervisor` green; `cargo clippy --all-targets` clean;
      `cargo fmt --check` clean.
- [ ] `slow_fsync_episodes_that_recover_never_escalate` and the unbroken-stall test green.
- [ ] Verified by a **separate** agent, not the implementer.

## Implementation notes

(implementer fills in: the exact predicate, why it cannot mask a hung thread, and the
before/after test evidence)
