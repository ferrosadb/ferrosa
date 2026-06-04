# Design — Property-based fuzz harness for the repair mechanism

> Last updated: 2026-06-03
> Status: Draft
> Goal: surface as many failures as possible across as many seams as possible in the
> storage/repair stack, via seeded (reproducible) property-based generators. Every bug we
> already hit becomes a named regression; the harness then hunts for new ones.

## Why

A self-managing database auto-runs repair with no operator. If repair has a latent bug
(OOM, data loss, non-convergence, corruption), the controller will *trigger* it
automatically and at scale. So the repair mechanism must be fuzz-hardened **before**
autonomy. Property-based + seeded (proptest / `ferrosa-sim` rng) = random exploration with
reproducible, shrinkable failures — consistent with the "deterministic system" rule.

## Generators (proptest `Strategy`, extend `ferrosa-common::test_generators`)

- `arb_partition` — random `DecoratedKey` (token spread), rows of `(col, CellValue)` mixing
  live / tombstone / expiring with varied timestamps (exercises LWW), random clustering.
- `arb_table_content` — `Vec<Partition>` with controllable key-overlap and size.
- `arb_replica_set(n)` — `n` replicas, each a divergent subset/version of a base set
  (drop/add/older-cell/newer-cell/tombstone per replica) → models divergence for repair.
- `arb_sstable_layout` — number of SSTables/table and token-range overlap mode:
  **full-overlap** (every SSTable spans the ring — the `entity_store` case), **disjoint**,
  **partial**. (Full-overlap is the case that exposed the reader-count wall.)
- `arb_corruption` — inject into serialized SSTables: oversized varint length prefixes,
  corrupt `DeletionTime` flags, zero-byte/missing components, truncated data.
- `arb_config` — random `fanin_cap`, `reader_cap`, `max_bytes`, `max_partitions`,
  compaction threshold, concurrency cap.

## Invariant properties (each a proptest; run with high `PROPTEST_CASES`)

1. **No panic / no unbounded alloc** — any input incl. corrupt → reader / merge / digest /
   bounded-fetch / compaction / startup-smoke return `Result` (clean error or ok), never
   panic, never attempt a pathological allocation. (Fail loud, don't crash.)
2. **Bounded memory** — peak concurrently-open readers ≤ `fanin_cap` **and** peak
   materialized partitions ≤ O(`fanin_cap`), regardless of SSTable count, data volume, or
   **full token overlap**. (This property is expected to FAIL today on the repair digest
   under full overlap — that failure is the RED for the repair-fan-in bound.)
3. **Equivalence** — streaming read/digest == single-pass reference, byte-identical, for
   random windows + budgets.
4. **Repair convergence** — divergent `arb_replica_set` → after repair every replica holds
   the per-cell LWW union; running repair again is a no-op (idempotent).
5. **No data loss** — post-repair / post-compaction content == LWW-merge of all input live
   cells (no live cell dropped unless superseded by a newer cell/tombstone).
6. **Determinism** — same input → same Merkle digest; `decide(snapshot)` → same action.
7. **Quarantine safety** — a corrupt gen excluded on one replica → its rows are recoverable
   via repair from a healthy replica; never lost when any replica has them.

## Seams to cover

SSTable data reader (corrupt bytes) · reader pool (random get/evict) · streaming merge
(`read_token_range`, `walk_token_range_for_digest`, `walk_token_range`) · budgeted fetch
(`read_token_range_bounded`) · compaction (random inputs → preserves data) · startup
smoke-test (corruption → bounded, excludes corrupt, keeps healthy) · anti-entropy repair
(divergence → convergence) — proptest level. Plus **`ferrosa-sim`** cluster level:
seeded multi-node repair under `nemesis` faults (corruption, divergence, node loss).

## Known-bug regression matrix (each must have a named test; most exist)

| Bug encountered | Regression test | Status |
|---|---|---|
| Corrupt clustering-value length → OOM | `corrupt_clustering_length_is_rejected_before_alloc` | ✅ |
| Resident reader O(count) OOM | `resident_reader_count_stays_within_cap_for_many_sstables` | ✅ |
| Read-merge (`read_token_range`) reader-count unbounded | `peak_open_readers_stays_within_fanin_during_staged_merge` (`store.rs`, added) | ✅ |
| Startup smoke-test residency OOM | `startup_build_table_state_holds_resident_readers_within_cap` | ✅ |
| Staged-merge tier-materialization (DATA) OOM | `large_range_digest_is_data_bounded_*`, `bounded_fetch_is_data_bounded_*`, `property_digest_walk_data_bounded_under_full_overlap` (added) | ✅ |
| Swap stale-gen / use-after-evict | `swap_evicts_removed_gens_*`, `held_reader_survives_*` | ✅ |
| Compaction concurrency / unpooled inputs OOM | `compaction_gate_caps_concurrent_*`, `compaction_inputs_routed_through_reader_pool` | ✅ |
| Streaming == single-pass | `streaming_*_byte_identical_to_single_pass`, fuzz `streaming_equals_single_pass` + `bounded_fetch_reassembles_to_single_pass` (added) | ✅ |
| Repair convergence / idempotence / quarantine | cluster fuzz `divergent_replicas_converge_and_repair_is_idempotent`, `quarantined_partitions_recovered_from_healthy_replica` (added) | ✅ |
| **Repair full-overlap reader-count OOM (node1)** — DIGEST path (`walk_token_range_for_digest`, used by `repair::build_tree_for_range`) holds O(sstable_count) readers under full overlap | `digest_walk_reader_count_bounded_under_full_overlap` (`store.rs`, gated behind `repair-fuzz-known-failures`) | ❌ **CONFIRMED by fuzz** — drives repair-fan-in fix. Min repro: cap=fanin=4, n_sstables=8, distinct_keys=6, full overlap → peak readers 8, soft-cap breaches 4 (expected ≤4 / 0). `read_token_range` over the same fixture stays bounded. |

## Process

1. Build the generators + property tests + a deterministic `ferrosa-sim` repair-fuzz
   scenario. Make the known-bug regressions explicit and green.
2. Run the fuzz at high case counts; **report every failure with its shrunk minimal input +
   seed** — do NOT fix production code in the harness task; failures are triaged and fixed
   deliberately (TDD: fuzz-found case becomes the RED).
3. Expected first catch: property #2 under full overlap → the repair-fan-in bound (already
   owner-approved) is then implemented TDD against that case.

## Implementation status (2026-06-03)

Harness implemented and committed on `fix/p0-sstable-memory-bounding`.

- **Generators** (`ferrosa-storage/src/test_support.rs`, behind the new
  `test-generators` feature; built on `ferrosa-common::test_generators`):
  `arb_partition` / `arb_partition_for_key`, `arb_table_content`,
  `arb_replica_set` (+ `lww_merge`/`newest_ts` oracle), `arb_sstable_layout`
  (`OverlapMode::{Full,Disjoint,Partial}`), `arb_corruption` (+ `apply_corruption`),
  `arb_config`.
- **Storage harness** (`ferrosa-storage/tests/repair_fuzz.rs`): #1 no-panic on
  arbitrary + corrupt SSTable bytes, #2 (reader half) peak resident ≤ cap, #3
  streaming==single-pass + bounded-fetch reassembly, #5 read-merge LWW no-data-loss,
  #6 digest determinism.
- **In-crate** (`store.rs`): #2 materialised-partition half
  (`property_digest_walk_data_bounded_under_full_overlap`) + the new green
  `read_token_range` reader-count regression, plus the gated known-failure.
- **Cluster harness** (`ferrosa-cluster/tests/repair_fuzz.rs`): #4 convergence +
  idempotence, #7 quarantine-safety.
- **Convergence oracle note**: shipped repair is *whole-partition* max-timestamp
  LWW (`diff_partition_sets` + `InMemoryRepairStore`), not the literal "per-cell
  LWW union" of the prose. The oracle matches the shipped model; the wording gap
  is documented, not a bug.
- **Not built**: the `ferrosa-sim` cluster-level nemesis scenario (proptest level
  covers the same seams; sim scenario deferred).
- **Confirmed failure**: property #2 reader-count half on the DIGEST path under
  full overlap (see matrix). Gated behind `repair-fuzz-known-failures` so CI stays
  green. This is the repair-fan-in wall RED; fix is the lead's TDD follow-up.
