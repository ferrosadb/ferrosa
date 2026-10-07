# T1 RED-side reproduction (t_80b2a90d)

The fix's GREEN tests live in the tree:

- `ferrosa/src/runtime.rs::tests::subsystem_worker_threads_stay_within_the_host_budget`
  — builds the real `RuntimeManager` in a child process with
  `FERROSA_RUNTIME_WORKER_BUDGET=4`, counts the process's live OS threads, and
  asserts they fit `4 + 8` slack. Deterministic, non-timing.
- `ferrosa-storage/src/self_heal/mod.rs::controller_tests::offloaded_tick_does_not_starve_a_coresident_liveness_task`
  — on a **current-thread** runtime, a co-resident liveness task wakes every
  20 ms; a 500 ms blocking tick is driven as a spawned task. A dedicated
  observer thread samples the wake gap (the liveness task stamps its last-wake
  `Instant` under a `std::Mutex`), so a gap that spans a parked executor is
  measured. Asserts `worst_gap < 300 ms`.

A green on the fixed revision alone proves nothing, so this directory carries the
RED side durably instead of as a prose claim:

- `graft.diff` — the **identical** test bodies in their pre-fix shapes, against
  the base revision `220aff3c` (the merge base this branch was cut from):
  the inline tick (`tokio::spawn(async move { controller.run_one_tick_guarded(); })`)
  and the pre-fix runtime plan (no `FERROSA_RUNTIME_WORKER_BUDGET`; the child
  still forces it, so the measurement is identical and only the code differs).
- `run.sh` — adds a detached worktree at `220aff3c`, applies `graft.diff`, and
  runs both tests. Exit 0 iff both FAIL (that is the required RED result).

```
scripts/t1-red-proof/run.sh            # BASE defaults to 220aff3c
```

## Measured numbers (18-vCPU macOS dev host, 2026-10-07)

| invariant | pre-fix (RED) | fixed (GREEN) | bound |
|-----------|---------------|---------------|-------|
| isolation: worst wake gap of a co-resident 20 ms liveness task during a 500 ms blocking tick | **695 ms** (inline parks the single executor thread for the block) | **38 ms** | `< 300 ms` |
| thread census: live OS threads after `RuntimeManager::new` in a child, forced budget = 4 | **28** (harness + the historical raft8/data8/cql8/background2 = 26 workers) | **6** (harness + 4 workers, split 1/1/1/1) | `<= 4 + 8 = 12` |

Both pre-fix runs exit 101 (`test result: FAILED`). Both fixed runs pass and print
`OFFLOAD_MEASURED … worst_gap_ms=38` / `RT_BUDGET_MEASURED budget=4 measured=6`.
