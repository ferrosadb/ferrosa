# FMEA — Bounded SSTable reader memory refactor

> Last updated: 2026-06-03
> Companion to [`p0-bounded-sstable-reader-design.md`](./p0-bounded-sstable-reader-design.md)
> Scoring: Severity / Occurrence / Detection each 1–10; RPN = S × O × D. Higher = act first.

This analyzes failure modes introduced by the LRU reader pool + staged read-merge. The
engine is a consensus-backed storage path, so correctness regressions are weighted as
critical (silent data loss/corruption outranks a crash, per the project fail-loud rule).

## Failure modes

| # | Failure mode | Effect | S | O | D | RPN | Mitigation (becomes a checklist item / test) |
|---|---|---|---|---|---|---|---|
| 1 | **Startup smoke-test opens every SSTable and holds them resident** | OOM during startup *before the pool is consulted* — the exact node2 failure observed | 9 | 8 | 3 | 216 | Smoke-test/validation must build *descriptors* and validate transiently (open → check → drop), never accumulate resident readers. Test: startup over N≫cap SSTables keeps resident ≤ cap. |
| 2 | **Descriptor key/token bounds wrong** → pruning skips an SSTable that holds matching data | Silent missing rows (data loss on read) | 10 | 3 | 5 | 150 | Derive bounds from the index footer the writer already emits; never approximate. Golden equivalence test: staged/pooled read == legacy full read for random windows. |
| 3 | **Staged multi-pass merge mis-orders dedup/LWW across tiers** | Silent corruption / wrong cell wins | 10 | 4 | 4 | 160 | Reuse `merge_partitions` + `apply_deletions` unchanged; tiers only change *when* sources open. Equivalence + property tests vs single-pass. |
| 4 | **Pool serves a generation removed by compaction swap** | Reopens deleted files / serves stale/deleted data | 8 | 4 | 5 | 160 | Evict `(table, gen)` from pool on every swap that removes the gen; key includes gen so a new gen can't collide. Test: swap then read → no stale gen. |
| 5 | **`Arc<SSTableReader>` pinned across `.await`** (repair streaming holds readers between chunks) | Resident > cap under load → OOM persists | 8 | 5 | 4 | 160 | Staged merge opens/releases readers *within* a pass; repair fetch must not hold readers across chunk awaits. Test: high-water open-reader gauge ≤ fanin_cap during a streamed repair. |
| 6 | **Mutex on LRU held across file-open IO** | Lock contention serializes all reads; latency cliff, possible stalls | 5 | 7 | 4 | 140 | Double-checked locking: lock→check→unlock→open→lock→insert. Never do IO under the pool mutex. Test: concurrent `get_or_open` of distinct gens proceeds in parallel. |
| 7 | **Soft-cap: all cached readers in use → cap exceeded** | Memory spikes above budget under high read concurrency | 7 | 4 | 4 | 112 | Staged merge bounds a single op to fanin_cap; bound *concurrent* heavy ops (reuse repair build semaphore pattern). Meter + log every soft-cap breach (fail-loud). |
| 8 | **Per-table pool instead of engine-wide** | N_tables × cap → unbounded in table count (agent_memory has ~45 tables) | 7 | 3 | 6 | 126 | Engine-wide pool, one budget shared across tables. (Design decision — confirm.) |
| 9 | **Cap too low → open/evict thrash** | Perf collapse (constant re-open), not OOM | 4 | 5 | 5 | 100 | Tune default vs 2 GiB budget; expose hit-rate + eviction metrics; default reader_cap=256 / fanin_cap=32 as starting point. |
| 10 | **Eviction drops a reader still mid-iteration** (refcount check race) | Use-after-evict / reopened mid-scan inconsistency | 9 | 2 | 5 | 90 | Only evict entries with `strong_count == 1`; the iterating path holds an `Arc` for its lifetime. Test: evict-pressure during an active scan does not disturb results. |
| 11 | **Compaction still opens its own fan-in (32) readers outside the pool** | Double-counts memory (pool cap + 32 compaction readers + concurrent tasks) | 6 | 4 | 4 | 96 | **ADDRESSED (Phase 6.6).** Compaction input opens routed through the engine-wide pool (`get_or_open`, keyed as the read path; abort-on-corrupt unchanged); concurrent merges capped by `CompactionGate` (`FERROSA_MAX_CONCURRENT_COMPACTIONS`, default 2). Metrics: `compaction_pool_input_opens_total`, `compaction_running_max ≤ cap`. Tests: `compaction_gate_caps_concurrent_holders`, `compaction_inputs_routed_through_reader_pool`. |
| 13 | **Read-merge bounds reader COUNT but materializes whole tiers of partition DATA** (the Phase-4 `stage_sstable_tiers` regression) | OOM-kill on a full-range digest/repair build over a table whose SSTables span the whole ring — exactly the `entity_store` Merkle-build OOM observed on a quiesced 3-node cluster. The fix for #5 (bound reader count) silently *introduced* this by collecting `O(table)` partitions per tier into a `Vec` up front. | 9 | 7 | 4 | 252 | **Stream, never materialize.** Remove tier materialization; k-way-merge one partition per source in flight so peak DATA = `O(open sources)`, not `O(table)`. For budgeted reads, check the budget before merging the next partition. Accept `O(open readers)` reader *count* under full overlap (structs are small; pool + compaction bound it) — strict reader-count bounding via external multi-pass is OUT OF SCOPE. **Test gate (this was the missing detection):** a *large-range data-bound* test asserting peak materialized partitions in flight is `O(fanin)` not `O(total partitions)` — `large_range_digest_is_data_bounded_not_table_bounded` + `bounded_fetch_is_data_bounded_not_table_bounded` (RED on tier code, GREEN on streaming). |

## Top risks to design out first (highest RPN)

0. **#13 tier-materialization data OOM (252)** — the highest-RPN mode, and the one that
   actually fired in production *after* the Phase-4 reader-count fix shipped. A mitigation that
   bounds reader *count* but not partition *DATA* is not a fix. The detection gap (no
   data-bound test, only reader-count tests) is why it regressed silently; the mandatory gate is
   a large-range *data*-bound test, not just a reader-count gauge. Fixed in Phase 6.5.
1. **#1 startup smoke-test residency (216)** — the actually-observed OOM. The pool is
   useless if startup still materializes every reader. This must be in P0a.
2. **#3 staged-merge dedup correctness (160)** and **#2 descriptor-bounds pruning (160)** —
   silent data corruption/loss. Gate with golden equivalence + property tests before any
   site migration is trusted.
3. **#4 stale-gen-after-swap (160)** and **#5 Arc-across-await (160)** — re-introduce the
   OOM or serve deleted data. Explicit eviction-on-swap + per-pass release.

## Decisions this FMEA forces (owner input needed)

- **D1 — Pool scope**: engine-wide (recommended; bounds total) vs per-table (simpler).
  FMEA #8 favors engine-wide.
- **D2 — Soft-cap behavior** when an op legitimately needs > cap readers: exceed-and-log
  (recommended; correctness over hard bound) vs hard-fail the op.
- **D3 — Default caps**: `reader_cap` and `fanin_cap` vs the 2 GiB node budget.
- **D4 — Scope of P0a**: include the startup smoke-test path (FMEA #1 says yes) — expands
  P0a beyond `StoreView`/read paths into the engine startup loop.
