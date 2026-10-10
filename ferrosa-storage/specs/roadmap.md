---
crate: ferrosa-storage
doc: roadmap
last_updated: 2026-09-26
---

# ferrosa-storage — Roadmap

Sourced from the FMEA gaps ([fmea.md](fmea.md)), the suite-level active-work
notes, in-code module markers (`self_heal`/`index` extension points), and the
dependency/usage review. The crate has **no in-source `TODO`/`FIXME`** markers —
open work lives in specs and the items below.

## Done (recent)

- **Streaming write-set staging (`write_set_stage.rs`).** `WriteSetStage` is the
  streaming twin of `WriteSetSpill`: driven one payload at a time as rows arrive, it
  keeps a bounded resident prefix and spills the rest to a private temp file, so the
  front end that BUILDS a write-set never holds the bulk. `entry(i)` reads back in
  append order (resident slice or mmap slice) and fails loud on an un-staged index.
  The resident limit is a streaming BUFFER SIZE externalized as
  `FERROSA_WRITE_SET_SPILL_THRESHOLD_BYTES` (default 8 MiB), never a cap — a larger
  write-set spills. See FMEA ST-WS-02.
- **Two public helpers for the Postgres foreign-key front end.** `add_btree_index(table,
  name, column_position)` (the declaration half) and `read_by_index_exists(table, name,
  key_bytes)` (the read half) let `ferrosa-postgres`'s `FOREIGN KEY` enforcement build and
  probe a single-column BTree index without naming `ferrosa-index`'s `IndexType`.
  `read_by_index_exists` stops at the first posting (one lookup, never O(result)) and
  propagates the same fail-loud errors as `read_by_index_each` — an undeclared or
  not-current index is an error, never an empty "no such parent" answer. See
  `ferrosa-postgres/specs/roadmap.md` for the enforcement feature.

## Now (highest value)

- **Operator-facing durability guidance (FMEA ST-1).** The default `Periodic`
  sync strategy trades a bounded loss window for throughput. Make the per-table /
  per-deployment choice explicit (when to pick `Batch`/`Group`), and surface the
  active strategy + observed sync interval in metrics/health so the window is
  never assumed away.
- **Strict reader fan-in bound under full token overlap (FMEA ST-8).** Resident
  readers are pooled and digest walks stream, but the strict open-reader bound
  for fully-overlapping repair digests is still a separate acceptance gate rather
  than enforced in-engine. Land the gate so anti-entropy repair cannot blow the
  fmem cgroup.

## Next

- **Compaction cancel safety — cancellation itself (T-021,
  `compaction-cancel-safety.md`).** T-022 (durable replacement record, with
  the output generation chosen and reserved before the record is written,
  forge t_cb6fa288) and T-023 (startup reconciliation) landed and, between
  them, close every window the T-020 crash-sweep (`cancel_harness_*`/
  `cancel_crash_sweep_*`) exercises — including window E (input retirement
  stopping partway through the input list) at the sweep's per-generation
  granularity, and the `AfterPromote` sub-window of C that T-022's initial
  landing left open (a crash between promotion and a since-removed
  post-promotion `output_gen` correction). No `cancel_crash_sweep_*` case is
  feature-gated any more; the `known-open-window` feature was deleted
  2026-09-26. **Remaining scope, still open:**
  - **T-021 itself: actual cancellation.** Nothing today can interrupt an
    in-flight compaction early (shutdown still joins every worker and waits
    out the merge) — the crash-sweep only proves what a *crash* leaves
    behind, not that a *voluntary* cancel is fast or possible at all.
- **Remove index artifacts with the generation they index (FMEA ST-58).**
  T-024 now removes all sidecars during compaction. The separate
  `delete_sstable_files` eviction path and historical debris still require
  investigation; previously each retired generation left its
  `.sidecar`, `FTI-` and `VEC-` files behind (5,122 FTI sidecars against 11 live
  SSTables per node on one cluster). The query path no longer reads them, but
  they cost disk, and a one-time sweep is needed for tables that already
  accumulated them. Deleting must respect S3-evicted generations whose sidecars
  are still live.
- **Proactive disk-pressure flush throttle (FMEA ST-4).** Beyond the
  `local_disk_free_reserve_bytes` fail-closed admission gate, add an earlier
  signal that throttles/accelerates flush+upload as local NVMe usage climbs, so
  writes degrade gracefully instead of hitting the hard reserve.
- **Self-heal controller: drain + converge actions.** The control loop +
  quarantine action (the first vertical slice) are implemented; compaction-driven
  drain, anti-entropy converge, and divergence detection are marked as explicit
  follow-ups in `self_heal/mod.rs`. Each remediation must stay bounded before the
  controller runs without an operator.
- **End-to-end PITR hardening (FMEA ST-10).** Snapshot/restore + commit-log
  archiving exist, and restore-on-boot (`restore/intent.rs`) now connects a
  requested restore to an actual node start — previously nothing did, so a
  node restarted cleanly and came back with pre-restore data. Remaining:
  - Wire `POST /api/restore` to persist an intent. It still validates and
    replies `202 "restart the node to complete restore"` while persisting
    nothing, so the HTTP path is a no-op; only the env-var path works.
  - Broaden restore validation coverage.
  - Fork/branch orchestration consumed by `ferrosa-dbaas`, including whether a
    manifest may reference SSTables under another object-store prefix (the
    prerequisite for a zero-copy fork).

## Later

- **TWCS / TTL-aware compaction levels.** STCS and UCS are implemented; a
  time-window strategy (or UCS with TTL-aware levels) for time-series tables.
- **Local NVMe bit-rot detection.** S3 objects are SHA-256-verified on read and
  flush self-readback exists, but there is no background scrub of local SSTables
  between flush and first read (FMEA ST-3 residual).
- **HVQ S3 spill-tier vector artifacts.** Page/range-readable `.qvec` resolver so
  vector artifacts need not fit on the compute node (per the storage topic spec's
  HVQ contract); current vector sidecars are whole-blob.
- **Grace-period GC + orphan sweep.** Confirm superseded-SSTable deletion grace
  and a periodic sweep of unreferenced S3 objects are bounded and observable.
- **Index reload read-cap pagination (t_1ec2e3fc).** `reload_indexes_from_system_schema`
  and `read_persisted_indexes` cap the `system_schema.indexes` scan at 10k rows
  and warn on truncation. The DROP TABLE cascade (t_ae06e925, landed) stops the
  table growing with orphans, but a legitimately huge index population still
  needs pagination instead of a cap.

## Recently landed

- **Stale secondary index after restart (ST-85).** A compaction swap retires
  its inputs from the index tracker, failed backfills log on their edges and
  are retried by the maintenance loop's healer, and not-current indexes show
  in `ferrosa_index_not_current` and `/readyz` `stale_indexes`. Next: move
  `IndexStateTracker` off its `RwLock` onto the lock-free `ArcSwap` pattern.

- **T-012 verification test isolation.** Digest-corruption tests disable the
  structural scan for their task only, preserving concurrent cancellation checks.

- **T-023 test isolation.** The retirement crash seam is task-local and resets
  on unwind or cancellation; unrelated compactions can run concurrently.

- **Streaming writer callers (T-039).** Flush and compaction target staged
  `Data.db`; compaction shares cancellation with the pump and cleans partial
  output after writer teardown. Startup sweeps legacy `Data.raw` scratch.

- **Compaction cancel-safety test harness (T-020, 2026-09-26).** Test
  infrastructure only, no behavior change: `CancelPoint` names every step in
  the compaction lifecycle table (`compaction-cancel-safety.md`), a
  `cancel_point!` hook (compiled to nothing outside
  `cfg(any(test, feature = "test-support"))`) is wired into the executor and
  `poll_compactions`, and `assert_cancel_invariants` checks I1-I4 against a
  `WriteOracle` model. First real use: a crash-twin subprocess sweep (CS2)
  that SIGABRTs a child process at each point and asserts the invariants
  after reopening — see `README.md` § Compaction cancel-safety harness.

- **Vector CREATE INDEX live-row backfill (2026-09-17).** Dynamic HNSW and HVQ
  registration now indexes rows in the active and flushing memtables before the
  new view is published, so switching ANN execution to the new index cannot
  turn a correct pre-index query into an empty result.

- **Crash-safe schema snapshot ownership (ST-17, 2026-08-27).** The engine no
  longer writes its table array to the registry-owned `schema.json`.
  `SchemaSnapshotStore` owns the discriminated, size-bounded registry format,
  cross-process lock, verified stage/fsync/rename publication, quarantine, and
  three retained generations. Standalone engine recovery uses the separate,
  atomically published `storage-schema.json`.

- **DROP INDEX live-state cleanup + index build path fixes (2026-07-03).**
  `StorageEngine::drop_index` now unwires declared indexes from the live
  `TableStore` and `IndexStateTracker` in Direct, pair, and Raft DDL paths,
  and remains idempotent when only tracker state exists locally. Re-registering
  an already loaded table now merges missing regular secondary-index
  declarations so sidecars loaded during `schema.json` boot preload remain
  visible to declared index reads.
  Keyed partition-index reads trust an empty consult only when the local storage
  tracker is authoritative and reports the index current with no pending SSTable
  backfill; otherwise they keep the bounded partition fallback. The local index
  backend also resolves engine table-dir SSTables under
  `sstables/<keyspace>.<table>` and writes generated sidecars beside those table
  SSTables.
- **DROP TABLE index-tombstone cascade (t_ae06e925, 2026-07-01).**
  `unregister_table` — the choke point for every DDL route (Direct, pair,
  cluster/Raft, and DROP KEYSPACE per-table) — tombstones the dropped table's
  `system_schema.indexes` registrations (`write_index_tombstones_for_table`)
  and sweeps its `IndexStateTracker` entries.
  `reload_indexes_from_system_schema` now returns
  `IndexReloadOutcome { restored, skipped }` and reports unresolvable rows via
  one summary warn + the `ferrosa_storage_index_reload_skipped_rows_total`
  counter (per-orphan detail demoted to debug). Pre-existing orphans are not
  GC'd automatically (a table can be mid-registration at boot); clean up
  manually with `DROP INDEX IF EXISTS`.

## Non-goals

- Query parsing, planning, protocol framing, or transport — those belong to the
  front-ends (`ferrosa-cql`, `ferrosa-postgres`, `ferrosa-sparql`,
  `ferrosa-graph`).
- Cluster routing/consensus — owned by `ferrosa-cluster`; this crate exposes the
  `DataStore` seam it routes through.

### Pump wiring acceptance (T-045)

File-backed sharded flushes stream each shard through the aligned pump into an owned staging directory, retain only component manifests, and publish the complete reader set in one view update. Guards remove unfinished staging after workers join. Wiring acceptance covers compressed/plain flush, compaction, restart, runtime pump settings, exact component bytes, and digest readback.

T-041 tests exercise actual engine flush/compaction stalls and scoped digest
readback checkpoints. Publication, WAL discard and compaction input retirement
wait for readback; a released flush completes with compaction still gated.
Admission bounds the active memtable; the separately retained flushing memtable
must be included in total memory accounting. These counters do not prove flat
process RSS. The isolated pump benchmark reports its own heap/RSS/throughput;
engine-level E1/E2/E3 RSS and Linux dirty-page/cgroup measurements remain live
acceptance evidence to collect, not inferred passes.

### Memtable write path: no per-write allocation

The production-default `SkipListMemtable::put` re-wrote the whole partition on
every row: it loaded the partition, deep-cloned it, merged one row, and
CAS-published a fresh `Arc`. This was structural (`arc_swap` exposes no
`DerefMut`, so CAS-publishing a changed value requires building a new one), and
it walked every row twice to re-size the partition — O(rows-in-partition) per
write, so filling one partition was O(N^2). On a live node replaying a commit
log it dominated: 52% of wall time in `Arc<Partition>::drop_slow` → jemalloc
free, ~95% CPU for 18+ minutes with no progress and no CQL listener. Measured:
1000 writes into a growing partition cost 2,109,500 allocations (~2109/write),
and the per-write cost grew with the partition.

The write now merges **in place** under a per-partition `parking_lot::RwLock`
(`Arc::make_mut` + `merge_row_into_partition`, mirroring `ShardedBTreeMemtable`),
and looks the key up by reference on the hot path so the key clone is paid only
on first insert. The `SkipMap` index stays lock-free; the lock is held for one
row's merge. Measured after: **10 allocations for 4000 writes** (amortized `Vec`
growth only). Regression guard:
`ferrosa-storage/tests/memtable_write_alloc_bound.rs` asserts the per-write cost
does not grow with the partition (a load-independent form of "no allocation per
write"); the harness builds input rows outside the measured window and runs as
one `#[test]` because the allocator counter is process-wide and not
thread-aware.

Read-ahead config: `FERROSA_COMPACTION_READAHEAD_BYTES`, default 1 MiB,
range 1..=256 MiB, rounded up to 4096 bytes. Invalid values emit ERROR and fall back;
valid normalization emits WARN with configured/effective sizes. Shutdown can
cancel a parked producer, then joins once the outstanding device call returns.

### Completed: T-024 local input retirement

Generation directories and flat components, including secondary/full-text/vector
sidecars, retire through durable hidden paths. Failures retain their intent and
retry on startup. The remaining T-060 extension is a grace period at the single
`remove_retired` reclamation seam; current retirement removes the files immediately.

### Completed: T-025 cancellation and operator control

Completed-result delivery waits on channel readiness, cancellation, and shutdown
without sleep polling. Disk-reserve pressure cancels the largest eligible task
while keeping admission fail-closed until space recovers. A shared tracker,
invalidatable submission tickets, and async DROP/TRUNCATE wrappers drain
finalization across CQL, pair, cluster, and Raft; sync storage calls return busy.

The authenticated operator API and ferrosa-ctl compaction stop cancel all
current tasks or one table scope without disabling future scheduling.

### Completed: T-026 S3 compaction recovery

The replacement record remains the durable cursor through upload confirmation,
manifest publication, and S3 input-delete enqueue. Startup rebuilds absent upload
ledger entries from replacement records, replays the uploads, and retries deletes
after manifest publication. Enqueue failures preserve the record for another
attempt. A mock-store crash sweep covers the S3 and pinned-local boundaries.

## jsonb (T-150)

Done: type threading. Remaining: None for T-150.
