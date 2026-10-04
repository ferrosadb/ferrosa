---
crate: ferrosa-storage
status: implemented
last_updated: 2026-10-03
executive_summary: >
  The single-node storage engine and durable substrate of the platform:
  memtable, write-ahead commit log, flush to BTI SSTables, S3 write-behind
  upload, STCS/UCS compaction, local NVMe cache + pinning, the secondary-index
  build pipeline, snapshot/PITR, and the corruption quarantine + self-heal
  controller. Every front-end and the cluster layer read and write through it,
  almost always via the Arc&lt;dyn DataStore&gt; boundary. Local disk is a
  write-behind cache; S3 is the authoritative durable store.
---

# ferrosa-storage — Architecture Overview

## Purpose & boundary

`ferrosa-storage` owns the **complete single-node data lifecycle**. It accepts
writes into an in-memory memtable, makes them durable through a segmented
write-ahead commit log, flushes to on-disk BTI SSTables, and asynchronously
uploads SSTable components to S3 for durability. Reads merge across memtable,
flushing memtable, and SSTables with cell-level last-write-wins semantics.

Automatic maintenance is volume-driven. Time alone cannot flush a sub-16-MiB
memtable, and retained-WAL pressure is charged only to the table pinning the
oldest closed segment after a fixed byte budget. Existing SSTable backlogs are
drained by bounded maintenance rounds even when no new writes arrive.

Its upstream boundary is the `DataStore` trait: front-ends hold
`Arc<dyn DataStore>` rather than `Arc<StorageEngine>`, so the same call sites
serve standalone (`LocalDataStore`) and cluster-routed deployments. Its downstream
boundary is `ferrosa-sstable` (BTI I/O) and `object_store` (S3). It knows nothing
about CQL/SQL protocol framing or query planning — those belong to the front-ends.

File flush and compaction stream into staged `Data.db` through the SSTable
writer. Compaction shares its task cancellation token with the pump, and a
borrowed staging guard removes incomplete output after the writer has dropped.
Startup removes legacy `Data.raw` scratch before generation discovery.

The test-only mid-retirement crash injection uses Tokio task-local scope, so
concurrent compaction tests and synchronous recovery cannot inherit a fault.
The scope unwinds with the injected future, including panic and cancellation.
Compaction resolves structural output verification once at task entry and
passes it explicitly to the executor; tests can choose a policy per task
without changing the process environment. Digest verification is unconditional.

## Module map

| Module | Responsibility |
|--------|----------------|
| `engine` (`src/engine.rs`, ~18.8k LoC) | `StorageEngine` + `StorageEngineConfig`: composition root; write/read/range/batch API, registration, snapshot/PITR orchestration, maintenance |
| `store` (`src/store.rs`, ~9.2k LoC) | `TableStore`: lock-free `ArcSwap<StoreView>` per table; flush serialization; reader-pool wiring; index/FTI sidecar flush; callback/async streaming global secondary-index reads; fail-loud bounded partition-keyed and geo index consults; secondary indexes on regular cells AND clustering-key components (`add_clustering_index`), live index removal (`remove_index`), incl. the partition-keyed consult `read_by_index_in_partition` (t_430c4188) |
| `memtable/` | `Memtable` trait; `SkipListMemtable` (default), `ShardedBTreeMemtable`; eager-index + vector-index hooks |
| `commitlog/` | Segmented WAL: `segment` (CAS alloc), `sync` (Batch/Periodic/Group), `reader` (replay), `archiver` (S3/PITR), `cdc`, `checkpoint`, `manifest` |
| `flush` | `FlushTarget` trait + `FileFlushTarget`/`InMemoryFlushTarget`; serialization-header construction |
| `merge`, `range_merger` | Read-path cell-level LWW merge; streaming range/token-range merge |
| `compaction/` | `CompactionExecutor`, STCS + UCS strategies, `CompactionGate`, validator (oracle + differential) |
| `upload/` | `UploadManager` (tokio task), `ObjectStoreConfig` (the one client + connection-pool settings), pending-upload log + replay across flat and generation-dir SSTable layouts; `download` (ranged-part component downloads, `FERROSA_S3_DOWNLOAD_PART_*`, `FERROSA_RESTORE_CONCURRENCY`); `stats` (optional `FERROSA_S3_STATS` layer, Prometheus + `system_observability.object_store_*` tables) |
| `cache`, `pin_config` | `LocalCache` LRU + pinning; NVMe `PinMode` |
| `index/` | Index state tracker (registered/pending/current completeness; compaction swaps retire inputs, failed/stalled backfills healed by `StorageEngine::heal_secondary_index_backfills`, ST-85), build scheduler, local/remote/off backends, artifact manifest, virtual table; `LocalBackend` resolves flat and engine table-dir SSTable layouts and writes sidecars beside table SSTables |
| `snapshot/`, `restore/` | S3 snapshot manager + restore manager + validation (PITR); `restore/intent.rs` carries the restore-on-boot intent (`FERROSA_RESTORE_*`) and the apply-once marker that keeps a reboot-surviving env var from re-restoring on every start |
| `quarantine`, `self_heal/` | Malformed-row quarantine sidecar; deterministic self-heal control loop + corrupt-SSTable detector |
| `accord/` | Per-shard conflict index + protocol log for Accord transactions |
| `timeseries/` | Ring aggregation, late-data, WASM aggregates, materialization |
| `data_store` | `DataStore` trait + `LocalDataStore` |
| `spill_budget` | Process RAM-budget detection (cgroup v2/v1 → `/proc/meminfo` → floor, injectable + cached) and the ORDER BY spill threshold (50% default; `FERROSA_RANGE_SPILL_THRESHOLD_{PCT,BYTES}`) |
| `external_sort` | Bounded-memory spilling external merge sort of CQL rows (`ExternalSorter`, cascade k-way merge, `MERGE_FANIN`) for unbounded `ORDER BY`; `SortedRows::into_disk_backed` parks a remainder on disk for CQL result cursors; fail-loud on spill/merge I/O |
| `metrics`, `virtual_tables`, `observer`, `subscription_observer` | Prometheus metrics; system virtual tables; write observers (CDC/SUBSCRIBE) |
| `batchlog` | Batchlog manager for atomic multi-partition batches |

## Data flow

**Write path** (front-end → durable): build a `Mutation` → `commit_log.append`
(CAS allocation into the active segment, durability governed by the sync
strategy) → `ArcSwap::load` the `StoreView` → `active.put` into one memtable
shard (cell-level merge-on-write). The async CQL `WritePath` applies per-table
pressure admission: in the soft zone it requests a background flush and waits
on that table's `Notify` until active-memtable capacity is released or a
bounded deadline expires; pressure is checked once more before dispatch. The
hard zone returns typed `Error::Overloaded`. Synchronous storage callers keep
the hard admission check. On flush: the request goes through the table's
rotation queue (one rotation at a time, no lock); a fresh memtable is swapped
in and the old one becomes `flushing`, its write gate is sealed and the writes
already inside it drain (new writes go to the new memtable at once). A
memtable a failed flush left in `flushing` (a list of sealed memtables) stays
there, readable, and the next rotation writes each sealed memtable to its own
SSTable, taking it out of the list in the same view change that installs it.
The flushing snapshot is serialized to a BTI SSTable via
`FlushTarget`; the new descriptor is prepended; index/FTI sidecars are built;
the SSTable components are submitted to `UploadManager` for S3 write-behind;
STCS/UCS is evaluated.

**Read path** (durable → front-end): `ArcSwap::load` (wait-free) a `StoreView` →
check active memtable → check flushing memtable → prune SSTable descriptors by
key/token bounds → open only candidate readers through the engine-wide LRU
reader pool (filling cold pages from `LocalCache`, falling back to S3) →
`merge_partitions` cell-level LWW newest-first. See [data-flow.md](data-flow.md)
for the mermaid diagrams.

Point reads publish maximum and above-threshold descriptor fanout. Above 32
SSTables the engine emits a rate-limited ERROR but continues through the fixed
reader-pool capacity, making degraded tables visible without multiplying log
volume or changing query results.

## Key invariants

1. **S3 is authoritative; local disk is a write-behind cache.** Cache eviction
   must never delete the only copy — manifest-pinned entries are never evicted.
   Uploaded-SSTable eviction is read-aware: a table read by a foreground query
   within `FERROSA_CACHE_HOT_WINDOW_SECS` (default 900; `0` disables) is never
   a candidate, and the rest go never-read first, then least recently read,
   then oldest write (FMEA ST-40). Anti-entropy repair reads do not count.
   A query over an evicted SSTable reads `Data.db` by paged ranged GETs and
   fetches only the small index components; it never downloads the whole
   generation (FMEA ST-51). Startup registers evicted generations
   remote-backed the same way (index components only; `Data.db` stays in the
   store) and a bounded background pass restores hot tables' generations while
   free disk allows. Compaction inputs and `FERROSA_RESTORE_EVICTED_MODE=full`
   still rehydrate in full.
   Local disk is always smaller than the database, so eviction and read-back
   run continuously, not only under pressure: `engine/cache_invariants.rs`
   drives an engine over an in-memory object store with a cache far smaller
   than its data and checks, after every flush+sync cycle, the cache bound (I1),
   every row through every read path (I2), identical rows across repeated
   evict/rehydrate cycles (I3), the same across restart (I4) and a loud error
   when the store has lost the objects (I5; FMEA ST-40, ST-41). It runs in
   PR CI; a larger randomized sweep is in `mod slow`.
   Periodic S3 sync skips incomplete generations before upload or manifest
   publication: all four required components (`Data.db`, `Partitions.db`,
   `Rows.db`, and `Filter.db`) must be present. This is a presence check;
   zero-byte `Rows.db` is a valid component.
   **Exception — local `file://` backend** (`FERROSA_LOCAL_STORE_PATH` /
   `[s3].local_path`): the local disk *is* the authoritative durable store, so
   `ObjectStoreConfig::is_local()` is threaded into `LocalCache` as `durable` and
   eviction is disabled entirely (`evict_if_needed` is a no-op). The local
   backend has no conditional-PUT (CAS) support, so manifest saves use the
   unconditional path; this is safe because a single node is the sole writer.
2. **Reads are wait-free; flush never blocks reads/writes.** All view
   transitions are `ArcSwap` compare-and-swaps derived from the current view;
   rotations run one at a time through a lock-free queue, and a writer that
   meets a sealed memtable moves to the next one instead of waiting.
3. **Cell-level last-write-wins everywhere.** Memtable merge-on-write, read-path
   merge, and compaction all resolve conflicts by `(column_index, timestamp)`;
   tombstones (partition/row/cell) suppress older data by `marked_for_delete_at`.
4. **Durability is governed by the sync strategy.** `Batch` fsyncs every
   write and `Group` makes the writer wait for its fsync; the **default
   `Periodic`** acknowledges before the fsync, bounded by refusal: a write
   gets `Error::CommitLogNotDurable` instead of an ack when the sync thread
   is dead, the last fsync failed, or the oldest unsynced write is older than
   `sync_stall_deadline` (2 s). A crash therefore loses at most that window of
   acknowledged writes (`max_delay`, 10 ms, while healthy). See FMEA ST-71.
5. **Index registration is replay-safe and complete for live rows.** Repeating
   the same index declaration preserves the active memtable index and its
   unflushed postings; a conflicting column position or index type fails loud
   instead of silently replacing it. A new declaration rotates the memtable:
   the rows written before CREATE INDEX leave with the frozen memtable, whose
   flush builds the new index's sidecar from them, so they are indexed without
   a fallback table scan and an empty ANN index is never published.
6. **Table registration is compare-and-install.** A schema replay that loses
   the table-map compare-and-swap merges declarations into the already-live
   store; it cannot replace active memtable rows or index postings.
7. **Schema updates preserve ordinal identity.** Regular columns are ordered by
   name, so `ALTER TABLE ADD` can shift a column's cell ordinal. `update_schema`
   remaps every positional index declaration (scalar, full-text, vector,
   filtered-predicate clauses) through the old schema's column name, flushes
   dirty pre-ALTER rows under their old serialization header and publishes the
   new schema in the same memtable rotation, and index backfills remap current
   column ordinals through each SSTable's stored header (ST-16).
8. **Malformed data is quarantined, not dropped or crashed on.** A row that
   fails cell/clustering validation at flush/replay is written to a durable
   `quarantine/*.jsonl` and the counter `FLUSH_QUARANTINED_ROWS_TOTAL`
   increments — non-zero in steady state is an alert.
9. **Compaction correctness is gated by a validator.** Oracle + differential
   checks confirm a compaction output is row-equivalent to its inputs.
10. **Automatic flushes are volume- or retained-WAL-driven.** A maintenance
    tick alone cannot create a sub-threshold SSTable. WAL pressure is computed
    from actual retained bytes and flushes only the table pinning the oldest
    closed segment; restart replay remains the durability path for smaller
    memtables.
11. **Backlog work is bounded and self-rescheduling.** Planning uses cached
    descriptor scalars with a configurable input cap, task/result queues have
    configurable per-worker capacities, and maintenance poll batch limits are
    runtime tunable. Positive values are validated and defaults preserve the
    existing backlog and maintenance behavior. See the flush and
    compaction runtime tuning table in `README.md`; settings reject values
    outside their documented practical ranges and fall back to defaults.
12. **A generation is verified before it can be discovered.** `flush_files`
    (shared by flush and compaction promotion) never promotes staged `.tmp`
    output to a live name until length checks, an fsync of the `.tmp`
    components, and a throwaway-reader readback walk over those same `.tmp`
    paths all pass. Any failure moves the `.tmp` set to `quarantine/` instead
    of leaving it under a name the next startup's `*-Data.db` generation scan
    would load (ST-27). Startup also sweeps stale `.tmp` sets and abandoned
    flush staging (`.sstable-staging/`, `.merge-spill/`) before that scan
    runs.
13. **No lock guards the table registry or a table's index declarations.**
    The table map is an `ArcSwap` (readers clone a table's `Arc` out before
    any callback, blocking send or long scan); a table's `IndexCatalog` is
    immutable and bound to its memtable; index DDL and `ALTER` rotate the
    memtable. Invariant: a memtable's index postings are exactly the sidecars
    its flush writes for the catalog it is bound to (ST-70).

## Concurrency

Shared state is published through `ArcSwap` and replaced by compare-and-swap
(`lockfree::update`, bounded at 1,000 lost races, then a loud error). Since
t_d938e6ae that covers the engine table map, each table's `StoreView`,
`IndexCatalog`, schema and NVMe `PinState`.

What replaced each per-table lock, and the test that pins the invariant it
protected (each goes red when its mechanism is sabotaged):

| Was | Now | Invariant / test |
|-----|-----|------------------|
| `write_barrier` (`RwLock<()>`) | `lockfree::WriteGate` per memtable: writers `fetch_add` in, a flush seals and drains | no write lands in a memtable after its flush snapshot: `no_write_lands_in_a_sealed_memtable` |
| `flush_guard` (`Mutex<()>`) | `TableStore::rotate`: a queue one caller runs for everyone, claimed by compare-and-swap; StoreView changes by CAS | one rotation at a time, queued requests coalesce: `flushes_queued_behind_a_running_flush_coalesce_into_one_rotation`; no view change lost: `sidecar_installs_and_flushes_lose_no_view_change` |
| `quarantined_sstables`, `index_unavailable_sstables` (`RwLock<HashSet>`) | `lockfree::SharedSet` | no lost insert/remove: `concurrent_set_changes_lose_no_update` |
| `vector_index_scopes` (`Mutex<HashMap>`) | `ArcSwap` of per-index `Arc` sets | no lost scope: `concurrent_vector_scope_records_lose_none` |
| `fulltext_sidecar_build_lock` (`Mutex<()>`) | per-sidecar claim in a `SharedSet`; a query that finds a build in flight scans instead of waiting | each sidecar built once: `concurrent_fts_sidecar_builds_build_each_sidecar_once` |

Locks that remain (outside this change's scope): `MissingSstableCache`
(`Mutex<HashMap>`, per table), and engine-wide `IndexStateTracker::states`,
`StorageEngine::pending_index_uploads` and the set-aside status.

Waiting that remains, by design: a flush waits (up to 30 s, then fails loud)
for the writes already inside the memtable it sealed; a rotation caller waits
for its own answer; DROP TABLE's `retire` waits for a rotation already
running on that table.

## Position in the dependency graph

A heavyweight internal hub. Depends on `ferrosa-cdc`, `ferrosa-common`,
`ferrosa-index`, `ferrosa-schema`, `ferrosa-sstable`. Depended on by `ferrosa`,
`ferrosa-cluster`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-graph`,
`ferrosa-index-builder`, `ferrosa-loadgen`, `ferrosa-postgres`,
`ferrosa-session`, `ferrosa-sparql`. See the root crate index for the full graph.

### Pump wiring acceptance (T-045)

File-backed sharded flushes stream each shard through the aligned pump into an owned staging directory, retain only component manifests, and publish the complete reader set in one view update. Guards remove unfinished staging after workers join. Wiring acceptance covers compressed/plain flush, compaction, restart, runtime pump settings, exact component bytes, and digest readback.

Shard workers collect moved component manifests directly into the fallible output vector; there is no intermediate vector of per-shard results.

T-041 tests exercise actual engine flush/compaction stalls and scoped digest
readback checkpoints. Publication, WAL discard and compaction input retirement
wait for readback; a released flush completes with compaction still gated.
Admission bounds the active memtable; the separately retained flushing memtable
must be included in total memory accounting. These counters do not prove flat
process RSS. The isolated pump benchmark reports its own heap/RSS/throughput;
engine-level E1/E2/E3 RSS and Linux dirty-page/cgroup measurements remain live
acceptance evidence to collect, not inferred passes.

Read-ahead config: `FERROSA_COMPACTION_READAHEAD_BYTES`, default 1 MiB,
range 1..=256 MiB, rounded up to 4096 bytes. Invalid values emit ERROR and fall back;
valid normalization emits WARN with configured/effective sizes. Shutdown can
cancel a parked producer, then joins once the outstanding device call returns.

### Durable input retirement (T-024)

After the committed replacement is visible, retirement hides each input before
reclaiming its files. A generation directory moves to `.retired-<generation>`;
flat layouts move Data.db first, followed by every generation-prefixed component,
including index sidecars. Directory fsyncs precede reclamation and follow removal.
Any failure retains the replacement intent and records a warning and counter.
The scoped `RetireInput` fault hook exercises failure after Data.db moved; restart
and subsequent retries finish the remaining components idempotently.

### Cancellation while delivering completed results (T-025 slice)

Result delivery uses crossbeam `select!` over the bounded result sender, the
task cancellation channel and executor shutdown. The unsent result remains owned
by the worker; cancellation drops direct-upload buffers, removes staged component
files and releases input claims. The pending-result counter is incremented inside
the selected send expression, before the receiver can observe the result.

### Disk-reserve cancellation (T-025 slice)

The write-admission reserve check requests cancellation of the largest registered
compaction by total input bytes. The same registry retains one token and size per
task; it excludes already-cancelled tasks and waits for an outstanding disk-reserve
cancellation to release before selecting another. Cancellation never substitutes
for the actual free-space check: the current write still fails closed while the
reserve is exhausted, and later writes can proceed once space recovers.

### Table DDL and compaction cancellation (T-025)

DROP TABLE, DROP KEYSPACE and TRUNCATE pause compaction admission, invalidate
submission tickets captured before input selection, cancel active tasks, and
await claim release before removing table data. The task registry and input
claims share one lock. Result publication and finalization notify async waiters;
no Tokio worker blocks on compaction completion. A committed replacement still
finishes its existing finalization before DDL proceeds. CQL, pair, cluster and
Raft application boundaries hold the pause through schema/storage changes.
Synchronous storage entry points return a busy error while work is active;
async callers use `unregister_table_and_wait` or `truncate_and_wait`.
Async APIs take `Arc<StorageEngine>` so an owned finalization job can survive a
request disconnect. TRUNCATE retains its pause through asynchronous S3 cleanup;
a dropped DDL waiter cannot strand a dequeued result or release its claim early.

### Durable S3 compaction completion (T-026)

The replacement record remains the recovery cursor after local input retirement.
It advances through `Retired`, `S3Uploaded`, `S3Manifested`, and
`S3DeletesEnqueued`; the pending-upload log carries replayable upload work and
is removed only after manifest publication. Startup reconstructs missing upload
entries from `Retired` or `S3Uploaded` records, then resumes idempotent deletes
from manifested records. A failed S3-delete enqueue is logged and leaves the
record for retry. Pinned and local-only compactions remove the record after
local retirement without attempting S3 work. Mock-store crash tests interrupt
pending-log, manifest-CAS, delete-enqueue, and pinned-retirement phases.
### Operator compaction stop (T-025)

Operator stop requests use `CancelReason::Operator` under the task registry lock.
They select all current tasks or one registered table, preserve the first recorded
cancellation reason, and leave admission enabled. Counts describe the registry at
request time; the caller does not wait for finalization or promise reclaimed disk.

### Compaction digest-failure retry

Digest or output-verification failures retry with exponential backoff (default
1 s initial, 60 s maximum). `FERROSA_COMPACTION_RETRY_BACKOFF_INITIAL_MS` accepts
1–60,000 ms and `FERROSA_COMPACTION_RETRY_BACKOFF_MAX_MS` accepts 1–600,000 ms;
out-of-range values clamp with a WARN, invalid integers use the default with an
ERROR, and a maximum below the initial delay is normalized up to the initial
delay. Three consecutive failures pause that table by default;
`FERROSA_COMPACTION_DIGEST_FAILURE_LIMIT` configures the threshold (1–100,
default 3; out-of-range values clamp with a WARN, invalid integers use the
default with an ERROR). The pause and retry streak are in memory: restart clears them, or an
operator can call
`StorageEngine::resume_table_compactions_after_digest_failures(table_id)` to
resume and reset the streak.

## jsonb (T-150)

`external_sort` payload accounting counts the canonical jsonb bytes (T-150); a document-sized value never accounts as 0.
