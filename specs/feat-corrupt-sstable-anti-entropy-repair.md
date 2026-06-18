# Corrupt-SSTable async anti-entropy repair (read-path fail-loud + failover)

Status: proposed. Branch: `correctness/raft-plan` (feature effort). Parent task: `t_21b2c1fa`.

## Problem

A read that hits a genuinely corrupt or missing SSTable must not fake `Ok(None)`. Today
`store.rs::with_retried_view` (~1313) exhausts `MAX_VIEW_RETRIES = 8` on a persistent
`sstable_open_failed`, logs `error!`, increments `view_retry_exhausted`, and then still
returns `Ok(result)` — possibly `Ok(None)`. That silently degrades a permanently-broken
SSTable to "key absent", violating the fail-loud priority ladder: a read that cannot
resolve a key it should see must `Err`, never `Ok(None)`.

Downstream of that storage gap, the coordinator has no failover for a local corrupt read:
at CL=ONE the read is local-only, so a corrupt local SSTable becomes a wrong/empty answer
to the client even when a healthy replica holds the data. And nothing schedules a repair to
refill the corrupt range, so the corruption is permanent until manual intervention.

This is a single feature effort spanning storage (the signal), the coordinator (failover +
async repair trigger), and repair (the refill), gated by a deterministic
"corrupt SSTable -> served from replica + repair triggered" test.

## Locked design decisions (verbatim — do not relitigate)

1. **ASYNC background repair:** serve the read from a healthy replica NOW; quarantine the
   corrupt SSTable and schedule anti-entropy (Merkle) repair to refill it in the background.
   Do NOT do synchronous foreground read-repair; do NOT block the read on repair.
2. **CL=ONE FAILS OVER to a remote replica:** a local corrupt-SSTable read error is treated
   like a failed replica; at CL=ONE the coordinator must read from another replica to serve
   the client (today CL=ONE is local-only — add the fallback). The read succeeds despite
   local corruption when any healthy replica has the data.
3. **SINGLE-NODE / RF=1 FAILS LOUD:** with no replica to repair from, the read returns `Err`
   (never fake `Ok(None)`). The 4 single-node resilience tests flip to assert fail-loud +
   that a repair was requested. (Exception: a read whose data is resolvable from a HEALTHY
   source — e.g. the memtable — still returns `Ok(Some)`; only an unresolvable read — result
   is `None` AND a source failed — errors. `memtable_data_survives_corrupt_sstable` must stay
   `Ok(Some)`.)
4. **ONE feature effort** across storage + coordinator + repair + tests, gated by a
   deterministic "corrupt SSTable -> served from replica + repair triggered" test.

## The transient-vs-corrupt line (critical)

A TRANSIENT compaction window — a file deleted mid-read that resolves within the bounded
`with_retried_view` retries (window closed by PR #143) — is **NOT** corruption: it must NOT
quarantine or trigger repair. ONLY **exhaustion** (all `MAX_VIEW_RETRIES` retries fail with
the SSTable still failing to open) = genuine corruption -> quarantine + repair.

Therefore the two existing race tests stay GREEN and untouched:
- `concurrent_read_during_compaction`
- `read_during_compaction_retries_against_new_view` (a.k.a. `mid_read_fetch_error_signals_view_retry`)

The boundary lives entirely at the retry-loop exit in `with_retried_view`: resolved-within-
retries -> `Ok`, no signal; exhausted-with-still-failing-open -> corruption signal up.

## Component map

```
ferrosa-storage                         ferrosa-cluster
  store.rs                                coordinator/read.rs              repair/
  ──────────────                          ───────────────────             ────────────
  read_with_view / read_clustering_row    coordinate_read (CL=ONE)        repair/coordinator.rs
    set sstable_open_failed       ───┐      local read errs (corrupt)       repair_table (~98)
  with_retried_view (~1313)          │      -> ReplicaRead::Failed          repair_initiated (~170)
    exhaustion: Err that IDENTIFIES  │      -> fall over to remote replica
    the corrupt SSTable (id/path) ───┼──>   serve client from replica     repair/scheduler.rs
    + quarantine it (skip on retry)  │      fire ASYNC repair (no block)    RepairScheduler / run_tick
                                     │            │
  self_heal/ (existing)              │            └──> RepairTrigger ──> refill quarantined
    SelfHealController                │                 (self_heal::refill)   SSTable's range from
    decide::Action::Quarantine        │                 cluster_view:          a verified-healthy
    ReplicaAwareClusterView           │                 verified-healthy       replica (Merkle
    RepairTrigger (FMEA #10 refill) <─┘                 replica gate           anti-entropy)
    FMEA #1: never quarantine the
    only copy (no healthy replica
    -> escalate/Err, not quarantine)
```

### Storage signal (ferrosa-storage)

- `read_with_view` / `read_clustering_row_with_view` already track `sstable_open_failed` and
  return `(Option<R>, bool)`. Carry the **failing descriptor** (SSTable id/path) up so repair
  can target it — the bool becomes/accompanies an identified corrupt-SSTable signal.
- `with_retried_view` (~1313): on failure-driven **exhaustion** (`stale_view_failed` still set
  after `MAX_VIEW_RETRIES` AND `result.is_none()`), return `Err` that identifies the corrupt
  SSTable, instead of `Ok(None)`. A refined version exists on branch
  `fix/read-retry-exhaustion-fail-loud` — reference it; re-implement cleanly here.
  - Do NOT turn a legitimate key-absence into an error. Only exhaustion caused by persistent
    `sstable_open_failed` errors. A result that resolves to `Some` despite a transient open
    failure (e.g. another source / the memtable held it) stays `Ok(Some)`.
- **Quarantine:** mark the corrupt SSTable so subsequent reads skip it (don't re-fail) and so
  repair knows the target. The existing `self_heal` module already owns quarantine:
  `decide::Action::Quarantine`, `ReplicaAwareClusterView`, the `RepairTrigger` refill port
  (FMEA #10), and the **FMEA #1 safety rail** (never quarantine the only copy — no healthy
  replica -> escalate, never quarantine). Wire the read-path exhaustion signal into this,
  reusing `scan_table_dir_for_corrupt` / `quarantine_corrupt_generation`.

### Coordinator failover + async repair (ferrosa-cluster/coordinator/read.rs)

- `ReplicaRead::Failed` (~253, ~572) already marks a replica error as failed.
- `coordinate_read` / `coordinate_read_with_filter` (~359, "CL=ONE local preferred", ~462)
  must fall over to a remote replica on the corrupt-SSTable error — today CL=ONE is local-only.
  The read succeeds when any healthy replica has the data.
- On serving-from-replica (or detecting a replica's corruption), fire an **ASYNC** repair
  (spawned, non-blocking) via the `RepairTrigger` so the read latency is unaffected.

### Repair refill (ferrosa-cluster/repair/)

- `repair/coordinator.rs` `repair_table` (~98) / `repair_initiated` (~170); `scheduler.rs`
  `RepairScheduler` / `run_tick`.
- The `RepairTrigger` (self-heal quarantine->refill port) plus `cluster_view`
  (verified-healthy-replica gate) trigger the refill of the quarantined SSTable's range from
  a healthy replica via Merkle anti-entropy. `cluster_view.rs` already gates quarantine on a
  verified healthy replica (FMEA #1: never quarantine the only copy).

## Test plan

Strict TDD; no `#[ignore]`, no silent test returns; fail-loud asserts.

### Single-node fail-loud + repair-requested (flip 4 existing resilience tests)

The 4 single-node corrupt-SSTable resilience tests flip from "returns `Ok(None)`/degrades" to
assert **fail-loud** (`Err`) **and** that a repair was requested:
- `memtable_data_survives_corrupt_sstable` — **stays `Ok(Some)`** (data resolvable from the
  memtable; only unresolvable reads error). This is the locked exception.
- `startup_warn_mode_excludes_corrupt_sstable_but_keeps_healthy_sstables_queryable` and the
  sibling startup/iteration-failure resilience tests — an **unresolvable** corrupt read (result
  `None` AND a source failed, RF=1, no healthy replica) returns `Err`, and a repair/escalation
  was requested (FMEA #1: no healthy replica -> escalate, never quarantine the only copy).

(Exact set of 4 confirmed against `ferrosa-storage/src/engine.rs` during implementation; the
two `corrupt`-named startup tests + `memtable_data_survives_corrupt_sstable` are the anchors.)

### Deterministic multi-node served-from-replica + repair-triggered (the gate)

The feature's gating test: a deterministic "corrupt SSTable -> served from replica + repair
triggered" scenario. Local replica's SSTable is corrupt (exhausts retries); a healthy remote
replica holds the row; at CL=ONE the coordinator **fails over** and serves `Ok(Some)` from the
remote; an **async repair** is triggered targeting the quarantined SSTable's range. Assert all
three: served-from-replica, quarantine recorded, repair-trigger fired.

### Compaction race stays green (the transient line)

`concurrent_read_during_compaction` and `read_during_compaction_retries_against_new_view`
(`mid_read_fetch_error_signals_view_retry`) stay GREEN and untouched — a transient compaction
window resolves within retries and must NOT quarantine or trigger repair.

## Green gate (before each commit)

`cargo fmt --check`; `cargo clippy -p <touched crates> --all-targets`;
`cargo test -p <touched crates>`; `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace`.
Commit only on green via explicit pathspec. If the coordinator-failover or repair-wiring seam
cannot reach correct + green, STOP and report where it stopped — do not fake-green or weaken a
test.

## References

- Parent task `t_21b2c1fa` (storage exhaustion fail-loud gap).
- Branch `fix/read-retry-exhaustion-fail-loud` (refined exhaustion-`Err` to re-implement).
- PR #143 (closed the transient compaction-window retry), #129/#130 (prior read-vs-compaction
  windows + `read_during_compaction_retries_against_new_view`).
- `ferrosa-storage/src/self_heal/` (existing controller, quarantine action, `RepairTrigger`,
  `ReplicaAwareClusterView`, FMEA #1 / FMEA #10 rails).
