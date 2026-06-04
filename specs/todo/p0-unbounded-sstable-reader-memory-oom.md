# P0 — Unbounded SSTable-reader memory causes OOM (streaming/bounded invariant violation)

**Status**: Root cause identified; fix in progress on `fix/bounded-sstable-reader-pool`.

**Severity**: P0. Nodes OOM-kill under their cgroup cap during startup / compaction /
repair. A storage engine must hold memory **bounded regardless of SSTable count**;
today it does not.

## Symptom

On the `ferrosa-memory` 3-node cluster, nodes whose tables had accumulated thousands
of small SSTables (left by the original compaction merge bug, since fixed by #70)
OOM-killed on startup and again under anti-entropy repair load, within the 2 GiB
per-node cgroup. node2 — *lean* (29 entity_store + 8 adjacency SSTables) — still
OOM'd during **startup**, before any repair RPC reached it. The OOM is recoverable
(restart-on-failure brings the node back) but recurs under load, and a full
convergence repair cannot complete.

## Root cause

Memory scales with **SSTable count**, which is unbounded:

1. **Resident readers (steady-state).** `StoreView.sstables: Arc<Vec<Arc<SSTableReader>>>`
   holds **one fully-constructed, permanently-resident `SSTableReader` per SSTable**
   (bloom filter, partition-index footer/bounds, header, per-reader decompressed-chunk
   LRU, and lazily-built partition-offset vectors). The active view keeps every reader
   alive for the table's lifetime, even when idle. `engine.rs:2293` already documents
   the failure mode: *"2,462 SSTables … reader overhead alone exceeded 1 GB."*
   → resident memory = **O(sstable_count)**.

2. **Read-merge fan-in (operation-time).** `read_token_range`,
   `read_token_range_bounded`, and `walk_token_range_for_digest` build a k-way merge by
   opening an iterator over **every** SSTable in the view at once
   (`sst_iters` sized `guard.sstables.len()`). A full-table token-range read or a repair
   Merkle build over a many-SSTable table therefore holds **O(sstable_count)** readers +
   their per-source in-flight partition simultaneously — the merge cannot evict mid-pass.

Compaction is *not* the violation: per-task fan-in is already capped
(`max_threshold = 32` files, `max_compaction_bytes = 512 MiB`, with tests) and its merge
is streaming (one partition per input). The violation is the resident-reader population
(1) and the read-merge fan-in (2).

## The invariant

Steady-state and per-operation memory must be **bounded by a configured cap**,
independent of how many SSTables a table has. Encoded as a TDD case:

- *Register a table with N ≫ cap SSTables and assert resident reader count ≤ cap.*
- *A full-range read / Merkle build over N ≫ cap SSTables holds ≤ cap readers open at
  any instant (fan-in bounded; multi-pass merge when N > cap).*

## Fix (two-part)

1. **Bounded LRU open-reader pool.** Make `StoreView` hold lightweight SSTable
   *descriptors* (gen, dir, key/token bounds) as the source of truth; open
   `Arc<SSTableReader>` on demand through an engine-wide LRU pool keyed by generation,
   evicting past a capacity cap. Resident reader memory becomes **O(cap)**. All ~30
   `guard.sstables` access sites migrate to pool lookups. Preserve the existing
   `sstables`/`sstable_ids`/`sidecar_indexes` parallel-length invariants by keying all
   three off the descriptor list.

2. **Bounded read-merge fan-in.** When a token-range read or Merkle build spans more
   than `cap` SSTables, merge in bounded passes (tournament / staged merge) so at most
   `cap` readers are open at any instant, mirroring compaction's `max_threshold`.

Both are required: (1) bounds idle/startup residency; (2) bounds a single large read.

## TDD plan

- `store`/`engine` unit test: `resident_reader_count()` ≤ cap after loading N ≫ cap
  SSTables (red today — equals N).
- `read_token_range`/`walk_token_range_for_digest`: assert peak concurrently-open
  readers ≤ cap for N ≫ cap (red today — equals N), and that results are unchanged vs.
  the unbounded path (equivalence).
- Pool unit tests: LRU eviction, on-demand open, cap honored, concurrent access.

## Notes

- Related hardening already landed on separate branches (held, not yet merged):
  `fix/sstable-bounded-length-allocation` (corrupt-length allocation guard) and
  `fix/repair-memory-and-correctness` (byte-bounded repair fetch). Neither addresses
  this resident-reader root cause.
- The original SSTable bloat is a *data* condition (merge bug, fixed by #70); this fix
  makes the engine *survive* such bloat with bounded memory instead of OOMing.
