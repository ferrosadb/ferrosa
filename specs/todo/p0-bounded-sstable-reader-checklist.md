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

## Phase 4 — Staged bounded read-merge — FMEA #3, #5 — DONE

- [x] staged merge helper `stage_sstable_tiers`: ≤ `fanin_cap` readers open at any instant (sequential per-tier open/drain/drop; multi-pass tiers); `FERROSA_READ_MERGE_FANIN` (default 32)
- [x] migrate `read_token_range`, `walk_token_range`, `walk_token_range_for_digest` (NOT `read_token_range_bounded` — lives on a held branch, absent here)
- [x] test: `peak_open_readers_stays_within_fanin_during_staged_merge` (peak resident ≤ fanin, soft_cap_breaches == 0)
- [x] equivalence tests: `staged_token_range_read_is_byte_identical_to_single_pass`, `staged_digest_walk_is_byte_identical_to_single_pass` (staged == single-pass, byte-identical, 6 random windows; dedup + LWW identical)
- [x] readers released per tier (no `Arc<SSTableReader>` held across `.await`; staged tiers drop readers before the next tier opens)
- [x] full lib suite green (785 passed); cluster `repair` tests green (54 passed)

## Phase 5 — Startup smoke-test bounded — FMEA #1 (top risk) — DONE

- [x] startup validation builds descriptors + validates transiently (open→check→drop), no resident accumulation. `load_existing_sstables_and_sidecars[_with_repair_mode]` now returns `Vec<SstableDescriptor>` (was `Vec<FileSSTableReader>`); each gen is opened through the engine-wide pool (`get_or_open`, keyed identically to the live read path via `SstableDescriptor::gen_num_for`), smoke-tested, reduced to a descriptor, then the Arc is dropped before the next gen. Excluded/quarantined gens are `remove()`d from the pool so they are never served. New `TableStore::new_with_descriptors_and_indexes` builds the `StoreView` from descriptors with no reader materialization (replaces the seeded-then-discarded `new_with_sstables_and_indexes` path at startup). `build_table_state` rewired to it.
- [x] test: startup over N≫cap SSTables → resident ≤ cap — `engine::tests::startup_build_table_state_holds_resident_readers_within_cap` (N=40, cap=4; asserts `resident_reader_count() <= cap` AND `peak_resident_readers() <= cap`). Proven RED→GREEN: holding the Arcs makes peak = N = 40 and the test fails. Corrupt-exclusion regression covered by `startup_warn_mode_excludes_corrupt_sstable_but_keeps_healthy_sstables_queryable` plus pool-eviction asserts added to the warn/quarantine smoke-test tests.
- [x] full lib suite green — 786 passed (was 785 + 1 new), 0 failed, 0 ignored; cluster `repair` 54 passed; clippy/fmt clean.

## Phase 6 — Swap correctness — FMEA #4, #11

- [ ] evict `(table, gen)` from pool on every compaction/flush swap removing the gen
- [ ] test: swap then read → no stale-gen reopen, no use-after-evict mid-scan
- [ ] (optional) route compaction input opens through pool / cap concurrent compactions

## Phase 7 — End-to-end verification

- [ ] full `ferrosa-storage` + `ferrosa-cluster` suites green; clippy/fmt clean
- [ ] rebuild node image from branch; deploy to fmem cluster
- [ ] repair on bloated node1 → `OOMKilled=false`, restart count stable, repair completes
- [ ] counts converge; viz/quorum sane
- [ ] update root-cause spec status → fixed; move design/fmea to implemented

## Phase 8 — Ship

- [ ] decide: fold into the combined hardening PR, or its own PR (likely own — large)
- [ ] push + open PR; report URL
