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
- [ ] **Owner decisions D1–D4 confirmed** (pool scope / soft-cap / default caps / P0a includes startup) — BLOCKS Phase 3+

## Phase 1 — ReaderPool module (isolated, no integration) — FMEA #6, #10

- [ ] `reader_pool.rs`: `ReaderKey=(TableId,gen)`, `Mutex<LruCache<ReaderKey, Arc<SSTableReader<FileReadAt>>>>`, `cap` from `FERROSA_SSTABLE_READER_CACHE_CAP` (default per D3)
- [ ] test: open-on-miss returns reader; second get is cache hit (no reopen)
- [ ] test: insert past cap evicts LRU
- [ ] test: do NOT evict an entry with `strong_count > 1` (in use)
- [ ] `get_or_open` uses double-checked locking — no file IO under the mutex
- [ ] test: concurrent `get_or_open` of distinct gens runs in parallel (no serialization)
- [ ] high-water gauge: `peak_resident()` / `resident()` for tests + metrics

## Phase 2 — Descriptors + accessor — FMEA #2

- [ ] `SstableDescriptor { gen, dir, min_key, max_key, min_token, max_token }`
- [ ] capture bounds from index footer at flush + compaction-swap + startup load (never approximate)
- [ ] `StoreView.sstables` → `Arc<Vec<SstableDescriptor>>` (keep parallel length-invariant w/ ids + sidecars)
- [ ] `TableStore::resident_reader_count()` accessor (delegates to pool)
- [ ] test (RED today): register table with N≫cap SSTables → `resident_reader_count() <= cap`

## Phase 3 — Wire pool into read paths (point/range first) — FMEA #2, #3

- [ ] `StorageEngine` owns `Arc<ReaderPool>`; `TableStore` gets a handle
- [ ] migrate point read + range read sites to `pool.get_or_open(desc)` (bloom-pruned)
- [ ] golden equivalence test: pooled read == legacy read for random windows
- [ ] full `ferrosa-storage` lib suite green

## Phase 4 — Staged bounded read-merge — FMEA #3, #5

- [ ] staged merge helper: ≤ `fanin_cap` readers open at any instant (multi-pass tiers)
- [ ] migrate `read_token_range`, `read_token_range_bounded`, `walk_token_range_for_digest`
- [ ] test: peak-open-readers gauge ≤ `fanin_cap` for N≫cap
- [ ] equivalence/property test: staged result == single-pass result (dedup + LWW identical)
- [ ] repair fetch releases readers per pass (no Arc held across chunk await)
- [ ] full lib suite green

## Phase 5 — Startup smoke-test bounded — FMEA #1 (top risk)

- [ ] startup validation builds descriptors + validates transiently (open→check→drop), no resident accumulation
- [ ] test: startup over N≫cap SSTables → resident ≤ cap
- [ ] full lib suite green

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
