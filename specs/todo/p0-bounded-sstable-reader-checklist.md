# Checklist — Bounded SSTable reader memory (P0)

> Living progress doc. Update the boxes as each step lands. Branch: `fix/bounded-sstable-reader-pool`.
> Design: [`../proposed/p0-bounded-sstable-reader-design.md`](../proposed/p0-bounded-sstable-reader-design.md)
> FMEA: [`../proposed/p0-bounded-sstable-reader-fmea.md`](../proposed/p0-bounded-sstable-reader-fmea.md)
> Root cause: [`p0-unbounded-sstable-reader-memory-oom.md`](./p0-unbounded-sstable-reader-memory-oom.md)
> Rule: every code step is red → green → fmt/clippy → crate tests. Commit only when green.

## Phase 0 — Design (done)

- [x] Root-cause spec written
- [x] Design doc + Mermaid (validated)
- [x] FMEA with RPNs + forced decisions D1–D4
- [x] This checklist
- [x] **Owner decisions D1–D4 confirmed**: D1 engine-wide pool; D2 exceed-and-log soft cap; D3 `reader_cap=256` / `fanin_cap=32` (env-tunable); D4 startup smoke-test bounded (Phase 5)

## Phase 1 — ReaderPool module (isolated, no integration) — FMEA #6, #10 — DONE (6ed0ef34)

- [x] `reader_pool.rs`: generic `ReaderPool<K,V>` (custom LRU, no new dep), `cap` from `FERROSA_SSTABLE_READER_CACHE_CAP` (default 256)
- [x] test: open-on-miss returns reader; second get is cache hit (no reopen)
- [x] test: insert past cap evicts LRU idle
- [x] test: do NOT evict an entry with `strong_count > 1` (in use) — soft-cap metered
- [x] `get_or_open` uses double-checked locking — no file IO under the mutex
- [x] test: concurrent `get_or_open` of distinct keys runs without deadlock
- [x] high-water gauge: `peak_resident()` / `resident()` / `soft_cap_breaches()`

## Phase 2 — Descriptors + accessor — FMEA #2 — DONE

- [x] `SstableDescriptor { gen, dir, min_key, max_key, min_token, max_token }` (`store.rs`)
- [x] capture bounds from index footer at flush + compaction-swap + load (`SstableDescriptor::from_reader`, byte_comparable decode; never approximate)
- [x] `StoreView.sstables` → `Arc<Vec<SstableDescriptor>>` (StoreView made non-generic; parallel length-invariant w/ ids + sidecars preserved in `check_invariants`)
- [x] `TableStore::resident_reader_count()` accessor (delegates to pool); `peak_resident_readers()` gauge
- [x] test: `resident_reader_count_stays_within_cap_for_many_sstables` (FileFlushTarget, cap=4, N=40)

## Phase 3 — Wire pool into read paths (point/range first) — FMEA #2, #3 — DONE

- [x] `StorageEngine` owns `SharedReaderPool<FileReadAt>`; `TableStore` holds a handle + opener closure (`FlushTarget::open_reader`); `attach_reader_pool` namespaces by table id
- [x] migrate point read (`read_limited_rows`, `read_clustering_row`, `visit_time_series_window_rows`) + range read (`count_range`, `range_iter`, `range_iter_projected`, `read_range_limited_rows`) sites to `pool.get_or_open` (token/key pruned by descriptor bounds)
- [x] evict `(table, gen)` from pool on compaction swap (`swap_compacted_sstables`, FMEA #4)
- [x] golden equivalence covered by full suite (785 lib tests incl. flush/compaction/restart roundtrips) + Phase-4 equivalence tests
- [x] full `ferrosa-storage` lib suite green (785 passed)

## Phase 4 — Staged bounded read-merge — FMEA #3, #5 — DONE, then SUPERSEDED by Phase 6.5

> **Superseded:** the `stage_sstable_tiers` helper introduced here bounded reader count but
> unbounded partition DATA and caused a P0 OOM (see Phase 6.5). It was removed; the read-merge
> paths now stream one partition per source. The boxes below record what Phase 4 did before the
> revert; the `peak_open_readers_stays_within_fanin_during_staged_merge` and
> `bounded_fetch_peak_readers_stays_within_fanin` reader-count gates were replaced by the Phase 6.5
> DATA-bound gates (a reader-count cap under full overlap is intentionally not enforced — see Phase 6.5).

- [x] ~~staged merge helper `stage_sstable_tiers`: ≤ `fanin_cap` readers open at any instant~~ (removed in Phase 6.5)
- [x] migrate `read_token_range`, `walk_token_range`, `walk_token_range_for_digest` (NOT `read_token_range_bounded` — lives on a held branch, absent here)
- [x] ~~test: `peak_open_readers_stays_within_fanin_during_staged_merge`~~ (replaced by `large_range_digest_is_data_bounded_not_table_bounded`)
- [x] equivalence tests (renamed `staged_*` → `streaming_*` in Phase 6.5; still == single-pass, byte-identical, 6 windows; dedup + LWW identical)
- [x] readers released per tier (no `Arc<SSTableReader>` held across `.await`; staged tiers drop readers before the next tier opens)
- [x] full lib suite green (785 passed); cluster `repair` tests green (54 passed)

## Phase 5 — Startup smoke-test bounded — FMEA #1 (top risk) — DONE

- [x] startup validation builds descriptors + validates transiently (open→check→drop), no resident accumulation. `load_existing_sstables_and_sidecars[_with_repair_mode]` now returns `Vec<SstableDescriptor>` (was `Vec<FileSSTableReader>`); each gen is opened through the engine-wide pool (`get_or_open`, keyed identically to the live read path via `SstableDescriptor::gen_num_for`), smoke-tested, reduced to a descriptor, then the Arc is dropped before the next gen. Excluded/quarantined gens are `remove()`d from the pool so they are never served. New `TableStore::new_with_descriptors_and_indexes` builds the `StoreView` from descriptors with no reader materialization (replaces the seeded-then-discarded `new_with_sstables_and_indexes` path at startup). `build_table_state` rewired to it.
- [x] test: startup over N≫cap SSTables → resident ≤ cap — `engine::tests::startup_build_table_state_holds_resident_readers_within_cap` (N=40, cap=4; asserts `resident_reader_count() <= cap` AND `peak_resident_readers() <= cap`). Proven RED→GREEN: holding the Arcs makes peak = N = 40 and the test fails. Corrupt-exclusion regression covered by `startup_warn_mode_excludes_corrupt_sstable_but_keeps_healthy_sstables_queryable` plus pool-eviction asserts added to the warn/quarantine smoke-test tests.
- [x] full lib suite green — 786 passed (was 785 + 1 new), 0 failed, 0 ignored; cluster `repair` 54 passed; clippy/fmt clean.

## Phase 6 — Swap correctness — FMEA #4, #11 — DONE

- [x] evict `(table, gen)` from pool on every compaction/flush swap removing the gen — already implemented in `swap_compacted_sstables` (`store.rs`): removed input gens are `reader_pool.remove(pool_key(desc))`'d before the new view is published; the new output gen is seeded via `seed_reader`. Verified by the new tests below (no production fix was needed — the swap path was correct; Phase 6 proves it).
- [x] test: swap then read → no stale-gen reopen, no use-after-evict mid-scan
  - `store::tests::swap_evicts_removed_gens_no_stale_reopen` (FileFlushTarget; 3 input SSTables primed into the pool, real file-backed compaction output via a standalone `FileFlushTarget`, swap removes all 3 inputs): asserts each removed gen's pool key is gone (`pool_contains_gen == false`), the output gen is seeded resident, post-swap reads return the post-compaction values (`merged{i}`, never stale input rows), removed gens are never reopened by reads, and only the 1 output reader stays resident.
  - `store::tests::held_reader_survives_concurrent_swap_eviction` (FileFlushTarget; 16-row partition): a reader `Arc` taken before eviction reads the full 16 rows, then `reader_pool.remove` evicts that gen, and the held `Arc` still yields identical complete rows (no panic / truncation / use-after-evict; `Arc::strong_count == 1` proves the pool released its ref while the scan keeps the reader alive). A fresh post-eviction store read reopens the gen and returns the same rows.
  - Added test-only `ReaderPool::contains(key)` + `TableStore::pool_contains_gen(gen)` (keys identically to the live path via `SstableDescriptor::gen_num_for`).
- [x] route compaction input opens through pool / cap concurrent compactions — **DONE in Phase 6.6** (FMEA #11). See below.

## Phase 6.6 — Bound compaction memory: pool-routed inputs + concurrency cap — FMEA #11 — DONE

> **Why now.** A repair on a node bloated with ~258 `entity_store` SSTables OOM-killed
> node1. Startup (Phase 5) and read-merge (Phase 6.5) are bounded, but the *compaction
> executor* opened its input SSTables directly via `FileReadAt::open` (a private path outside
> the engine-wide pool) and ran up to `worker_count` (≤4) tasks concurrently with no global
> cap. Peak compaction memory was therefore unbounded in `(concurrent tasks × per-task
> inputs)`, competing with repair/read memory under the 2 GiB cgroup.

**Concurrency model found:** `CompactionExecutor` spawns `worker_count` (`FERROSA_COMPACTION_WORKERS`,
default `available_parallelism().clamp(1,4)`) worker threads, each draining its own mpsc
queue. Tasks run **concurrently** across workers — so a global cap was required, not just a
per-node serial guarantee.

- [x] **Fix #1 — pool-routed input opens.** `execute_task_inner` now takes an
  `Option<&SharedReaderPool<FileReadAt>>`. Each input is still strictly validated/sized
  (`ensure_compaction_component` + rehydration; **any missing/corrupt input still aborts the
  whole task** — abort-on-corrupt is unchanged regardless of cache state), then the opened
  `SSTableReader` is obtained through `pool.get_or_open((table_id, gen_num_for(id)), …)` —
  keyed **identically to the live read/startup path** — so compaction's resident input readers
  are shared with and evictable by the same global bound. Readers held as `Arc` for the merge
  (soft cap never evicts an in-use reader). The engine creates the pool *before* the executor
  and passes it via `CompactionExecutor::with_reader_pool` (all three production constructors +
  the test constructor). `metrics::COMPACTION_POOL_INPUT_OPENS_TOTAL` /
  `ferrosa_storage_compaction_pool_input_opens_total` counts pool-routed opens.
- [x] **Fix #2 — concurrency cap.** `CompactionGate` (counting semaphore over
  `parking_lot::{Mutex,Condvar}`) caps concurrent merges at
  `FERROSA_MAX_CONCURRENT_COMPACTIONS` (default **2**). A worker acquires a permit *before*
  running the merge and `inc_compaction_running` is bumped only after the permit is taken — so
  `ferrosa_storage_compaction_running_max` reflects tasks actually executing, never those
  blocked at the gate. `acquire` polls the `stop` flag (bounded 100 ms wait) so shutdown never
  deadlocks a gated worker.
- [x] tests (`compaction::executor::tests`):
  - `compaction_gate_caps_concurrent_holders` — 8 threads × 50 iters contend on a cap-2 gate;
    asserts max concurrent holders ≤ 2 (the invariant `running_max ≤ cap` relies on).
  - `compaction_gate_unblocks_on_shutdown` — a worker blocked on an exhausted gate returns
    `None` once `stop` is set (no deadlock).
  - `compaction_inputs_routed_through_reader_pool` — after a pool-routed compaction, both input
    gens are resident in the pool (keyed as the read path keys them), the pool-routed-open
    counter advanced once per input, and all input partitions survive the merge (correctness).
  - `concurrent_compaction_cap_default_is_small_and_positive` — default cap = 2, parsing ≥ 1.
- [x] `execute_task` (direct, non-pooled) retained only for tests + the `compaction-validator`
  harness (gated `#[cfg(any(test, feature = "compaction-validator"))]`); production workers use
  the pool-routed path.
- [x] full `ferrosa-storage` lib suite green (797 passed = 793 + 4 new, 0 failed, 0 ignored);
  cluster green with CI skips; clippy/fmt clean.

## Phase 6.5 — Tier-materialization OOM regression: stream, don't materialize — FMEA #3/#5/#13 — DONE

> **Regression found in Phase 4.** `stage_sstable_tiers` (added in Phase 4) bounded the
> reader *count* but **unbounded the partition DATA**: each tier collected every in-range
> partition of its SSTables into a `Vec<Partition>` up front. The digest Merkle build calls
> `walk_token_range_for_digest` over the **full token range**, so on a table whose SSTables
> each span the whole ring (`entity_store`/`typed_edges`) every tier materialized ~the entire
> table → peak `O(total partitions)` → node OOM-killed. Live proof: a quiesced 3-node cluster
> OOM-killed both repair peers during `entity_store` Merkle build. This regressed the digest
> path from its original (pre-Phase-4) **streaming k-way merge** ("one partition per source in
> flight regardless of table size") to tier-materialization.

- [x] **Fix — stream, never materialize tiers.** Removed `stage_sstable_tiers` entirely. In
  `walk_token_range_for_digest`, `read_token_range_bounded`, and `walk_token_range`, open one
  pooled streaming reader per overlapping SSTable (token-pruned by `overlaps_token_range`) and
  k-way-merge **one partition at a time**: peek smallest key across all sources, cell-merge the
  same-key partitions across sources (`merge::merge_partitions` + `apply_deletions`, unchanged),
  emit/hash that ONE partition, advance. Peak materialized partitions = `O(open sources)`, never
  `O(table)`. For `read_token_range_bounded` the byte/count budget is checked **before** the next
  partition is merged, so peak working set is `max_bytes` + one in-flight partition.
- [x] **Full-overlap vs disjoint.** When SSTables are token-disjoint the merge naturally only
  decodes the few sources covering the current key. When every SSTable spans the full range
  (`entity_store`/`typed_edges`), sub-ranging cannot reduce fan-in — so we open all overlapping
  readers and stream. That keeps DATA bounded (one partition per source); reader *structs* are
  small and the resident pool + compaction bound how many overlap. **Strict reader-count bounding
  under full overlap (external multi-pass merge) is explicitly OUT OF SCOPE** — DATA bounding is
  what fixes the OOM. Documented inline in `walk_token_range_for_digest`.
- [x] **New test gate (the gap that let the regression through): large-range DATA-bound.**
  - `store::tests::large_range_digest_is_data_bounded_not_table_bounded` — 40 full-overlap SSTables
    × 12 keys (480 partition copies on disk, 12 merged in range). Asserts the test-only
    peak-in-flight gauge (`store::inflight`) stays `O(open sources)` for both `walk_token_range_for_digest`
    and `walk_token_range`, NOT `O(480)`.
  - `store::tests::bounded_fetch_is_data_bounded_not_table_bounded` — 30 full-overlap SSTables × 10
    keys; tight count budget; asserts peak in-flight is budget-bounded, not `O(300)`.
  - **RED→GREEN proven**: temporarily re-staging overlapping SSTable partitions into a
    `PartitionSource` (the old regression shape) drives the digest gauge to 480 and fails the test;
    the streaming fix passes.
  - Instrumentation: test-only `store::inflight` peak gauge + `PartitionSource` wrapper (compiles
    away in production via `#[cfg(test)]`).
- [x] **Equivalence tests kept/renamed** (correctness on the consensus path): `streaming_token_range_read_is_byte_identical_to_single_pass`,
  `streaming_digest_walk_is_byte_identical_to_single_pass`, `bounded_fetch_is_byte_identical_to_single_pass_read_token_range`
  (== single-pass `read_token_range` golden across 6 windows / all budgets).
- [x] Removed now-dead `effective_read_merge_fanin` + `read_merge_fanin_override` field +
  `set_read_merge_fanin_for_test` (the fanin knob only selected the tier path, which no longer exists).
  `read_token_range` (Vec-returning, `limit`-bounded) audited: it never routed through
  `stage_sstable_tiers` and is bounded by its caller-supplied `limit`; no production caller passes an
  unbounded limit over a large range, so left as-is.
- [x] full `ferrosa-storage` lib suite green (793 passed, 0 failed, 0 ignored — unchanged count); cluster green with CI skips; clippy/fmt clean.

## Phase 7 — End-to-end verification

- [ ] full `ferrosa-storage` + `ferrosa-cluster` suites green; clippy/fmt clean
- [ ] rebuild node image from branch; deploy to fmem cluster
- [ ] repair on bloated node1 → `OOMKilled=false`, restart count stable, repair completes
- [ ] counts converge; viz/quorum sane
- [ ] update root-cause spec status → fixed; move design/fmea to implemented

## Phase 8 — Ship

- [ ] decide: fold into the combined hardening PR, or its own PR (likely own — large)
- [ ] push + open PR; report URL
