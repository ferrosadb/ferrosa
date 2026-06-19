# Test Specification: MemIndex, Transactional 2i, Sidecar Files, SUBSCRIBE Dual Timestamps

**Sprints:** S4 (MemIndex, Sidecar, SUBSCRIBE), S6 (Transactional 2i READ_2I Algorithm)

---

## 1. MemIndex Unit Tests (S4.1)

| Test | What It Proves | How |
|------|---------------|-----|
| `mem_index_apply_gc` | MemIndex entries are queryable within their timestamp window and are garbage-collected when `flush_gc` advances past their `accord_ts`. | Insert entry `(column=age, value=25, partition_key=pk1, accord_ts=10)`. Lookup `(age, 25, read_ts=15)`. Assert: returns `pk1`. Call `flush_gc(flushed_up_to_ts=12)`. Lookup again with `read_ts=15`. Assert: returns empty (entry GC'd because `accord_ts=10 <= 12`). |
| `mem_index_update_replaces` | Updating an indexed column on the same partition removes the old value's index entry and installs the new one, preventing stale index pointers. | Insert `(age, 25, pk1, ts=10)`. Then insert `(age, 30, pk1, ts=15)` — same partition, new value. Assert: `lookup(age, 25)` does NOT return `pk1`. `lookup(age, 30)` returns `pk1`. The old value's entry was removed. |
| `mem_index_delete_removes` | DELETE on a partition clears all of that partition's index entries, so deleted rows are never returned by index lookups. | Insert `(age, 25, pk1, ts=10)`. DELETE `pk1` at `ts=15`. Assert: `lookup(age, 25)` does NOT return `pk1`. The `by_partition` reverse lookup for `pk1` is cleared. |
| `mem_index_range_scan` | Range queries over indexed values return only partitions whose indexed value falls within the requested range (inclusive/exclusive boundaries). | Insert entries: `(age, 20, pk1)`, `(age, 25, pk2)`, `(age, 30, pk3)`. Range scan `age 22..28`. Assert: returns `pk2` only. |
| `mem_index_timestamp_filter` | MemIndex respects `read_ts` visibility — entries with `accord_ts` after the read timestamp are invisible to the reader, providing snapshot isolation. | Insert `(age, 25, pk1, ts=10)`, `(age, 25, pk2, ts=20)`. Lookup `(age, 25, read_ts=15)`. Assert: returns `pk1` only (`ts=10 <= 15`). `pk2` (`ts=20 > 15`) is excluded. |
| `mem_index_empty_lookup` | Lookups on non-existent column/value pairs return an empty result set gracefully, not an error or panic. | Lookup on a column/value with no entries. Assert: returns empty vec, not an error. |
| `mem_index_by_partition_tracks_values` | The `by_partition` reverse map correctly tracks all indexed values for a partition, enabling DELETE to clean up all index entries without scanning the forward map. | Insert `(age, 25, pk1)` and `(name, "alice", pk1)`. Assert: `by_partition[pk1] = {25, "alice"}`. This is used by DELETE to clean up all index entries for a partition. |

---

## 2. Atomic MemIndex + Memtable Apply (S4.2 — FM6, RPN 189)

| Test | What It Proves | How |
|------|---------------|-----|
| `mem_index_memtable_atomicity` | The Apply handler writes base data and index projections atomically — a reader never observes memtable data without the corresponding MemIndex entries (or vice versa). | In the Apply handler, call `memtable.write()` and `mem_index.apply()` for the same txn. Assert: both complete or neither completes. No interleaving with another Apply handler on the same shard. Use a single-threaded shard executor to verify. |
| `mem_index_crash_recovery` | The commit log contains enough information (both base data and index projections in `AccordApplied`) to restore memtable and MemIndex to a consistent state after a crash. | Write `AccordApplied` entry to commit log (includes both base data and index projections). Crash before `memtable.write()` completes. Replay commit log. Assert: both memtable and MemIndex are restored from the single commit log entry. Consistency check: every row in memtable has corresponding MemIndex entries. |
| `mem_index_no_interleave` | Shard-level serialization prevents partial-state observations between concurrent Apply handlers on the same shard. | Two Apply handlers for txns T1 and T2 on the same shard. Assert: T1's memtable+MemIndex writes complete atomically before T2's start (or vice versa). No partial state where T1's memtable is written but T1's MemIndex is not. |

---

## 3. MemIndex Flush GC (S4.3)

| Test | What It Proves | How |
|------|---------------|-----|
| `mem_index_flush_gc_boundary` | `flush_gc` removes exactly those entries whose `accord_ts` is at or below the flushed watermark, retaining all entries above it. | Insert entries at `ts=5, 10, 15, 20`. Call `flush_gc(flushed_up_to_ts=12)`. Assert: entries at `ts=5` and `ts=10` removed. Entries at `ts=15` and `ts=20` retained. Boundary is inclusive: `ts=12` would be removed if it existed. |
| `mem_index_flush_gc_by_partition_cleanup` | `flush_gc` also cleans the `by_partition` reverse map, removing stale value references and entirely removing partitions with no remaining entries. | Insert `(age, 25, pk1, ts=5)`. `flush_gc(ts=10)`. Assert: `by_partition[pk1]` is also cleaned up (remove the value `25` from `pk1`'s tracked values). If `pk1` has no remaining values, remove `pk1` from `by_partition` entirely. |
| `mem_index_flush_gc_idempotent` | Calling `flush_gc` multiple times with the same watermark is safe and produces no errors or double-removals. | Call `flush_gc(ts=10)` twice. Assert: second call is a no-op, no errors. |

---

## 4. Eager Index Build (S4.4)

| Test | What It Proves | How |
|------|---------------|-----|
| `eager_index_build_on_flush` | Flushing a memtable for an indexed table immediately schedules a sidecar index build at high priority, so the MemIndex-to-persistent-index gap is minimized. | Configure a table with a secondary index. Flush a memtable to SSTable. Assert: index build is scheduled immediately at `Priority::High` (not deferred to compaction). The index build produces a sidecar index file for the new SSTable. |
| `eager_index_build_layer4_bounded` | The number of unindexed SSTables (Layer 4 in READ_2I) stays bounded even under rapid flush activity, keeping 2i query scan costs predictable. | Flush 3 SSTables in quick succession. Assert: index builds are queued and processed. At any point, the number of unindexed SSTables (Layer 4 in READ_2I) is at most 1-2 (the ones currently being built). Not unbounded. |
| `eager_index_build_after_compaction` | Compaction output SSTables also receive eager index builds, and obsolete sidecar index files from input SSTables are cleaned up. | Compact two SSTables into one. Assert: the merged SSTable also gets an eager index build. The old sidecar index files are deleted. |

---

## 5. Sidecar `.accord` File (S4.9 — OQ5 Decision)

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_sidecar_write_on_flush` | Every flush that includes Accord transaction data produces a `.accord` sidecar file alongside the SSTable, keyed by `TxnId`, containing each transaction's result bytes. | Flush a memtable containing rows from 3 Accord transactions (T1, T2, T3). Assert: a `.accord` sidecar file is written alongside the SSTable. File contains: `{T1.txn_id -> T1.result, T2.txn_id -> T2.result, T3.txn_id -> T3.result}`. File is keyed by `TxnId`. |
| `accord_sidecar_recovery_read` | The `.accord` sidecar provides the result bytes needed by recovery coordinators to re-apply a transaction on a shard that missed it, without re-executing the transaction. | Transaction T1 is Applied on shard A but not shard B. Shard A's SSTable has a `.accord` sidecar with T1's result. Recovery coordinator reads the sidecar to get T1's result and re-applies to shard B. Assert: result bytes match. Shard B now has T1 applied. |
| `accord_sidecar_gc` | Once all shards have applied a transaction (ExclusiveSyncPoint reached), its entry is removed from the sidecar to reclaim space. Empty sidecar files are deleted entirely. | All shards confirm T1 is Applied (ExclusiveSyncPoint reached). Assert: T1's entry is removed from the `.accord` sidecar file. If the sidecar has no remaining entries, the file is deleted. |
| `accord_sidecar_s3_upload` | The `.accord` sidecar is treated as a companion file to its SSTable during S3 write-behind — both are uploaded and downloaded together. | When an SSTable is uploaded to S3, the `.accord` sidecar is uploaded alongside it as a companion file. Assert: S3 upload includes both the SSTable and the sidecar. On cache miss, both are downloaded together. |
| `accord_sidecar_normal_read_ignores` | Normal data-path reads (SELECT queries) never open or read `.accord` sidecar files, avoiding unnecessary I/O on the hot read path. | Normal SELECT query reads an SSTable that has a `.accord` sidecar. Assert: the sidecar is NOT opened or read during normal data reads. Only recovery reads the sidecar. |
| `accord_sidecar_empty_flush` | Flushes with no Accord transaction data do not produce spurious empty sidecar files. | Flush a memtable with no Accord transactions (all data came from non-Accord writes before Accord was enabled). Assert: no `.accord` sidecar file is created. |

---

## 6. SUBSCRIBE Dual Timestamps (S4.10 — OQ3 Decision)

| Test | What It Proves | How |
|------|---------------|-----|
| `subscribe_dual_timestamps` | SUBSCRIBE events carry both `accord_ts` (logical ordering timestamp) and `apply_ts` (wall-clock application time), giving consumers the information needed for both causal and temporal ordering. | SUBSCRIBE to a table. Write a row via Accord (`accord_ts=100`, applied at `wall_clock=200`). Assert: the SUBSCRIBE event contains both `accord_ts=100` and `apply_ts=200`. |
| `subscribe_accord_ts_ordering` | SUBSCRIBE events are emitted in `accord_ts` order (logical/causal), not in the order they were applied on this node, so consumers see a causally consistent stream. | Write T1 (`accord_ts=10`, `apply_ts=200`) and T2 (`accord_ts=20`, `apply_ts=150`). T2 was applied before T1 on this node (network delay). Assert: SUBSCRIBE emits T1 before T2 (ordered by `accord_ts`, not `apply_ts`). |
| `subscribe_backward_compat` | Adding `accord_ts` to SUBSCRIBE events is wire-compatible with old consumers — the new field is additive and does not break existing deserialization. | Old consumer that doesn't understand `accord_ts` receives events. Assert: events still contain all existing fields. The new `accord_ts` field is additive — old deserialization code ignores it. |
| `subscribe_apply_ts_sort` | Consumers that explicitly opt into `apply_ts` ordering receive events in wall-clock order, preserving backward-compatible behavior for use cases that need it. | Consumer explicitly sorts by `apply_ts`. Assert: events come in wall-clock order (T2 before T1 in the example above). This is the "old behavior" opt-in. |

---

## 7. Transactional 2i: READ_2I Algorithm (S6.1-S6.6)

| Test | What It Proves | How |
|------|---------------|-----|
| `read_2i_five_layer_merge` | The 5-layer merge produces a complete result set by unioning candidates from all layers and subtracting deletions, so no committed data is missed regardless of where it resides. | Set up all 5 layers: (1) in-flight txn writing indexed value, (2) committed-not-applied txn, (3) MemIndex entry, (4) unindexed SSTable, (5) persistent index. Query via 2i. Assert: results include candidates from all 5 layers. Deletions from MemIndex and ConflictIndex are subtracted. |
| `read_2i_no_phantom_reads` | The 2i read path provides snapshot-consistent results — a row either appears in all re-reads at the same `read_ts` or in none, never flickering. | Writer inserts row with `age=25` in transaction T1. Concurrent reader queries `WHERE age=25` via 2i. Assert: reader either sees the row (if T1's `accord_ts <= read_ts`) or doesn't (if T1 is still in-flight). Never sees a phantom (row appears then disappears on re-read). |
| `commit_index_indexed_writes` | PreAccept populates the ConflictIndex with indexed-column writes so that in-flight transactions are visible to 2i queries before they are applied. | PreAccept a transaction that writes column `age=25` on partition `pk1`. Assert: `ConflictIndex.indexed_writes[age][25]` contains this txn. A 2i query for `age=25` finds this in-flight write. |
| `2i_dep_wait_latency` | Dep-wait on in-flight transactions resolves quickly under normal conditions, keeping 2i query tail latency acceptable. | 2i query hits a pending dep (in-flight txn on the indexed column). Assert: dep-wait resolves within 5ms P99 under normal conditions (no failures). Measure: instrument dep-wait duration. |
| `2i_unindexed_sstable_scan` | Unindexed SSTables (Layer 4) are scanned correctly via bloom filter + BTI scan during the index-build window, and stop being scanned once the persistent index is available. | Flush an SSTable. Index build is in progress (not yet complete). 2i query must scan the unindexed SSTable using bloom filter + BTI scan. Assert: results include rows from the unindexed SSTable. After index build completes, the SSTable is no longer scanned (Layer 4 moves to Layer 5). |
| `2i_eventual_mode` | The `eventual` consistency option for secondary indexes trades freshness for speed by skipping volatile layers (1-4) and reading only the persistent index. | `CREATE INDEX WITH OPTIONS = {'consistency': 'eventual'}`. Assert: 2i reads skip Layers 1-4 (ConflictIndex, MemIndex, unindexed SSTables). Only the persistent index (Layer 5) is queried. Faster but may return stale results. |
| `2i_concurrent_write_read_consistency` | After an Accord transaction commits, subsequent 2i queries observe the committed write — no stale reads after commit confirmation. | Writer: `BEGIN TRANSACTION; INSERT INTO t (id, age) VALUES (1, 25); COMMIT`. Concurrent reader: `SELECT * FROM t WHERE age = 25`. Assert: after writer's Accord transaction commits, the reader's 2i query returns the row. Never returns stale pre-insert state after the commit. |
| `2i_delete_removes_from_all_layers` | DELETE via Accord removes the row from 2i results across all layers — MemIndex and ConflictIndex track the deletion until the persistent index is rebuilt without the deleted entry. | Row with `age=25` exists in persistent index. DELETE the row in an Accord transaction. Assert: 2i query for `age=25` no longer returns the row. Deletion tracked in MemIndex and ConflictIndex until persistent index is rebuilt. |
