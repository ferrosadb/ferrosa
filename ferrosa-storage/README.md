# ferrosa-storage

> The single-node storage engine: memtable → commit log → flush → SSTable →
> S3 write-behind, plus compaction, local cache, NVMe pinning, secondary-index
> pipeline, snapshot/PITR, and the corruption quarantine + self-heal controller.

## What this crate is

`ferrosa-storage` is the **storage substrate** of the platform — the largest and
most critical crate in the workspace (~73k LoC across ~95 source files). It owns
the full single-node data lifecycle: writes land in an in-memory memtable, are
made durable through a write-ahead commit log, are flushed to BTI SSTables on
local NVMe, and are then asynchronously uploaded to S3 (the durable store).
Local disk is a write-behind cache; S3 is authoritative.

Every query front-end (CQL, Postgres, SPARQL, Graph) and the cluster layer reach
data through this crate, almost always via the `Arc<dyn DataStore>` indirection
(`LocalDataStore` in standalone/pair mode; a cluster-routing impl otherwise).

## What's implemented

- **Memtable** — sharded write buffer behind the `Memtable` trait. Default build
  uses `SkipListMemtable` (crossbeam skiplist, feature `skiplist-memtable`);
  `ShardedBTreeMemtable` (64 `parking_lot::RwLock` shards) is the alternative.
  Per-partition merge-on-write (cell-level LWW, tombstone merge). When a legacy
  whole-value collection and path-keyed collection elements meet during replay
  or a live update, the merge expands the whole value into a deletion sentinel
  plus sorted element cells. Flush therefore sees one collection
  representation and cannot panic on a live pathless complex cell.
- **Commit log** (`commitlog/`) — segmented WAL with CAS-based lock-free
  allocation, forward-linked sync markers, crash-recovery replay, CDC reader,
  S3 archiver for PITR, and per-table checkpoints. Three sync strategies
  (`Batch`, `Periodic`, `Group`); **default is `Periodic`** → a bounded
  durability window (see FMEA).
- **Flush** (`flush.rs`, `store.rs`) — `TableStore` composes active/flushing
  memtables + SSTable descriptors behind a single `ArcSwap<StoreView>`. Flush is
  serialized by a per-table `Mutex`; reads/writes are never blocked. Optional
  `write_verify` self-readback after every flush. The durability barrier
  (`fsync_components`) fsyncs a generation's component files **concurrently** on
  a shared, bounded flush pool (`flush_executor`, a rayon `ThreadPool` whose
  width is `FERROSA_FLUSH_PARALLELISM`, default = host parallelism), then
  barriers all before the single directory fsync — the barrier ordering
  (component bytes durable before the rename entry) is preserved, and any
  component fsync failure fails loud without the directory fsync. The pool caps
  concurrency across *all* concurrent flushes, so flush parallelism is a
  capacity-aware knob rather than a per-flush thread count.

  **Publication safety — verify before promote (`publication-safety.md` M2):**
  `FileFlushTarget::flush_files` (used by both flush and compaction promotion)
  renames staged output to `.tmp`, checks every component's length, fsyncs the
  `.tmp` components, then opens a THROWAWAY reader over the `.tmp` paths and
  walks every partition — only after that succeeds does it promote to live
  names and fsync the directory. A refusal at any of those steps moves the
  whole `.tmp` set into `quarantine/` (WARN, `sstable_publication_refused_total{reason}`)
  instead of returning with output under a name generation discovery would
  load. Before this ordering, a readback failure was detected only AFTER
  promoting to a live name, so a corrupt SSTable could enter the live view
  next to the WAL replay of the same rows (FMEA ST-31). Startup sweeps stale
  `.tmp` sets and abandoned `.sstable-staging`/`.merge-spill` staging into
  `quarantine/`/removed, before generation discovery runs
  (`StorageEngine::load_existing_sstables_and_sidecars_with_repair_mode` calls
  `flush::sweep_stale_flush_staging`; `FileFlushTarget::new`/`new_starting_at`
  call it too, as a safety net for callers outside table startup).

  **Digest verification on published bytes, unconditional (`publication-safety.md`
  M2 step 4 / M3, T-012, FMEA ST-32):** between the `.tmp` fsync and the
  structural readback walk above, `flush_files` recomputes `Digest.crc32` by
  reading the `.tmp` Data.db back from disk (a reused 1 MiB buffer, never a
  whole-file `Vec`; `POSIX_FADV_DONTNEED` on Linux afterward so the check does
  not refill the page cache the write pump bypasses) and compares it with the
  producer's value. A mismatch quarantines under
  `PublicationRefusedReason::DigestMismatch` and refuses publication, same as
  every other verify-before-promote failure. This closes the gap the readback
  walk alone left open: that walk proves the file *decodes*, not that it holds
  the bytes the producer actually wrote, so a length-preserving corruption (bit
  flip, swapped block, stale bytes from a segment recycled before its write
  completed) could pass it. Because compaction promotes through this same
  `flush_files`, the digest check is unconditional for compaction too —
  `FERROSA_COMPACTION_VERIFY_OUTPUT` continues to control only compaction's
  separate row/partition count walk, never this check. Checksum loading
  (`Digest.crc32`/`CRC.db`) is centralised in
  `ferrosa_sstable::reader::{load_checksums_if_present, load_checksums_for_generation}`
  and every production file-backed open path calls it: this crate's flush-open
  helpers (already did, since T-011), the compaction executor's input open, the
  local index-build backend, and `ferrosa-ctl`'s `sstable` reader (both
  previously opened readers with checksums never loaded at all). An SSTable
  predating T-011 still opens everywhere, treated as "not checked" and logged
  once per generation, never as an error.

  **Streaming writer callers (T-039):** file flush and compaction open staged
  `Data.db` directly. Compaction passes its task cancellation token into the
  writer so backpressure wakes on cancellation; a borrowed staging guard joins
  writer teardown before deleting abandoned output. Startup removes legacy
  `Data.raw` scratch along with abandoned staging directories, preserving live
  generation files.

  **Automatic-flush admission (t_889b0d9a):** maintenance cadence alone never
  creates a tiny SSTable. The age trigger requires at least 16 MiB (or the
  configured flush threshold when smaller), while the size/backpressure
  triggers are unchanged. A table may also flush to release retained WAL, but
  only after eight segment-equivalents of actual closed-log bytes are pinned
  and only when that table owns the oldest closed segment. Smaller dirty
  memtables remain WAL-backed and stream through normal replay after restart.
  The fixed 64-batch regression produced 63 SSTables before this policy and one
  afterward; no SSTable or commit-log encoding changed.

  The dominant flush cost is **SSTable encoding** (BTI trie build + serialize +
  compress) — ~97% of flush wall time, historically single-threaded per SSTable
  and the real write-throughput floor (fsync was only ~0.3%). For tables with
  **no secondary indexes**, `flush_sharded` splits the token-sorted memtable
  snapshot into contiguous token-range shards and encodes each into its own
  SSTable **in parallel** on the `flush_executor` pool, then publishes all shard
  SSTables together (they are non-overlapping by construction, so they read and
  compact like any other SSTable set). Measured ~3× sustained write throughput
  vs. single-threaded encode; the sharded flush is gated to no-index tables for
  now (per-shard index splitting is a later increment). Indexed tables keep the
  single-SSTable path. Shard count = `desired_flush_shards` (≥512 partitions per
  shard, capped at pool width).
- **Compaction** (`compaction/`) — `CompactionExecutor` on dedicated
  `std::thread` workers behind a global `CompactionGate`. STCS (default) and UCS
  (CEP-26 density-based) strategies. A compaction validator (oracle + differential
  checks) gates correctness.
  **Parallelism auto-tune (t_a0f922a3):** worker count and the concurrent-merge
  cap now derive from the node's CPU count and configured memory (cgroup v2/v1
  limit, else system RAM) — at most `cpus`, and at most `memory/2 ÷ 256 MB`
  concurrent merges (keeping peak compaction memory under half the limit),
  clamped to 8. Since compaction streams a partition-group at a time (per-task
  memory ≈ widest partition × inputs, not the whole SSTable), the old fixed cap
  of 2 was over-conservative. `FERROSA_MAX_CONCURRENT_COMPACTIONS` /
  `FERROSA_COMPACTION_WORKERS` still override; the resolved values are logged at
  startup.
  **Direct-read + read-ahead input (on by default):** compaction
  opens each input `Data.db` as a private cache-bypassing scan
  (`FileReadAt::open_scan`: O_DIRECT / `F_NOCACHE`) with a background read-ahead of
  `FERROSA_COMPACTION_READAHEAD_BYTES` (default 1 MiB, cap 256 MiB; two windows
  resident per input), so a compaction pass neither evicts query-hot pages nor
  issues one small `pread` per compression chunk (CASSANDRA-15452). Scan readers
  are **not** parked in the shared reader pool (the live read path does point
  reads a one-pass window cannot serve), so this mode's residency is bounded by
  `inputs × 2 windows` per task instead of the pool. Turn it off with
  `FERROSA_COMPACTION_DIRECT_READ=0`, or every direct-I/O path at once with
  `FERROSA_DIRECT_IO=0` (the specific switch wins); both are read at run time, so
  no rebuild is needed. A value that is not a boolean is ignored with a WARN and
  the default (on) applies. Watch
  `ferrosa_sstable_direct_read_fallbacks_total`: non-zero means the file system
  rejected O_DIRECT and the bypass is inactive.
  **Tombstone purge (`gc_grace_seconds`):** compaction drops a deletion marker
  (partition deletion, row deletion, cell tombstone) only when BOTH hold: its
  `local_deletion_time` is older than `now - gc_grace_seconds`, and its timestamp is
  below the minimum timestamp of any data outside the compaction that could overlap
  (SSTables not in the task whose token range overlaps the inputs, plus the active
  and flushing memtables, via `Memtable::min_timestamp`). Otherwise dropping it would
  resurrect older data. `CompactionTask::purge` carries the policy, computed by
  `StorageEngine::purge_policy_for` at submission; a schema with no `gc_grace_seconds`
  means no purge. Kill switch: `FERROSA_COMPACTION_PURGE_TOMBSTONES=0`. A pathless
  collection tombstone is kept while element cells it shadows remain. If every
  partition purges away, one is written unpurged (an empty output cannot be swapped
  in) and counted. Metrics: `ferrosa_storage_compaction_purged_markers_total`,
  `..._purge_held_back_total`, `..._purge_policy_errors_total`.
  **Flush fix:** a partition holding only a partition-level delete (no rows, no static
  row) is now flushed; it used to be dropped as empty, losing the delete.
  Existing backlogs drain without waiting for another flush: every maintenance
  poll consumes at most eight results and admits at most eight table tasks, then
  schedules another round when work completes. Descriptor-cached, constant-size
  metadata selects no more than `min(max_threshold, 64)` of the smallest inputs
  without opening/decompressing the whole backlog. Per-worker task capacity is one and
  result capacity is two; saturation releases claims and leaves input files
  live for the next bounded retry.
  **Legacy-format rewrite (t_a0f922a3):** `SSTableMetadata::legacy_format` flags
  SSTables whose key bounds are not byte-comparable-decodable (older
  Cassandra-shaped files that can store a wide partition's rows out of clustering
  order). `strategy::legacy_rewrite_tasks` schedules these for a rewrite
  **regardless of size tier** (size bucketing would otherwise leave a lone legacy
  file unselected indefinitely), chunked by the same `max_threshold` /
  `max_compaction_bytes` bounds as STCS, one task per chunk; `maybe_compact`
  excludes files a strategy task already covers to avoid overlapping repairs.
  **Compaction output is always monotonic:** because `merge::merge_partitions`
  returns a single-source partition in its on-disk order, the executor runs
  `merge::ensure_partition_rows_sorted` on every merged partition (a cheap O(n)
  `is_sorted` check for the already-sorted common case), so even a 1-input legacy
  rewrite re-sorts each partition — permanently fixing the on-disk order the
  streaming read path assumes.
  **Promote directory fsync before input eviction (T-001, FMEA ST-30):**
  `promote_compaction_output` renames the staged output into
  `sstables/<table>/<gen>` and then fsyncs `sstables/<table>/` itself, reusing
  the same barrier `flush.rs` uses for its own promoting renames
  (`FileFlushTarget::fsync_dir`, now `pub(crate)`). `poll_compactions` only
  calls `evict_local_input_sstable_files` when promotion returns `Ok`, so a
  directory-fsync failure structurally prevents input eviction — without this,
  a crash between the rename and the unlinks could persist the unlinks while
  losing the rename, destroying both the inputs and the output. On failure the
  rename is undone when the filesystem allows it; if even that fails, the
  output is left as a visible orphan (a disk leak, not data loss — swept by a
  future startup reconciliation, T-023) and the caller still sees `Err`.
  `evict_local_input_sstable_files` no longer silently discards unlink errors:
  unexpected failures are WARN-logged (a missing file is not an error). Full
  atomic, fsynced retirement of the whole input generation is T-024.
  **Cancellable compaction (T-021, `compaction-cancel-safety.md` C1):**
  `try_submit` creates one `ferrosa_common::CancelToken` per task, checked
  (unconditionally — in every build, not only tests) at every input open, the
  top of each merge-loop partition, before `finish_to_directory`, before
  `flush_files`, once per readback-verify partition, and once more in
  `poll_compactions` immediately before promoting — the last point at which
  cancelling is free (nothing has been promoted, opened, or observed yet); a
  cancelled checkpoint removes whatever this task staged so far (loud errors,
  never `let _`) and releases the input claim through the existing paths.
  **After promotion, cancellation is recorded but not honoured** — T-022 owns
  that commit point. `CompactionExecutor::shutdown` cancels every live token
  *before* joining workers (reaching both actively-merging tasks and
  completed-but-not-yet-promoted results still sitting in the result queue),
  so shutdown waits out one checkpoint interval rather than a whole merge.
  The executor's task and result queues, and the worker loop's own wait, are
  `crossbeam_channel::bounded` with a blocking `select!` against a shutdown
  channel — no `recv_timeout` poll interval, so an idle worker exits shutdown
  immediately. Metric: `compaction_cancel_latency_seconds` (cancel-call to
  observed-`Err`). The T-020 cancel-point harness
  (`compaction/cancel_harness.rs`) now also registers each task's live token
  by table-id scope, so a test's cancel-point hook can call
  `cancel_harness::cancel_now` to actually cancel — not merely record having
  reached — a `CancelPoint`.
  **Durable replacement record + startup reconciliation (T-022/T-023, FMEA ST-34):**
  `compaction::intent::CompactionIntentRecord` is a JSON file at
  `sstables/<table>/.compaction-<id>.intent` — `{task_id, output_gen,
  output_digest, inputs, phase}`, `phase` one of `Promoting → Swapped →
  Retired`. `poll_compactions` writes it (fsynced file + fsynced table dir)
  **before** promoting the staged output; that write is the commit point. A
  failure after commit but before retirement (reader-open, sidecar-merge, or
  swap) rolls back under the record — removes the promoted output directory,
  fsyncs, deletes the record — via `StorageEngine::rollback_compaction_intent`,
  leaving the inputs untouched (`compaction_intent_rollback_total`). This
  closes the gap ST-27's tombstone purge opened: retiring inputs one at a time
  with no cross-input atomicity meant a crash between two retirements could
  delete the input holding a purged tombstone while the input holding the row
  it shadowed survived, resurrecting that row. Startup reconciliation
  (`StorageEngine::reconcile_compaction_intents`, run per table before
  generation discovery, alongside the existing `.promote-*` staging sweep)
  rolls an incomplete record back if its output was never promoted,
  quarantines the output on a `Digest.crc32` mismatch (T-011 format) while
  keeping the inputs live, and otherwise rolls forward: retires every listed
  input still on disk — idempotent, since a missing component is a no-op — so
  a crash mid-retirement always finishes. This does not replace
  `upload::PendingUploadsLog`: that log still separately drives S3
  upload/manifest/delete recovery on the same path as before; the replacement
  record is the source of truth only for the local promote/swap/retire
  sequence. Metrics: `compaction_intent_rollback_total`,
  `compaction_reconcile_rolled_back_total`,
  `compaction_reconcile_rolled_forward_total`,
  `compaction_reconcile_digest_mismatch_total`,
  `compaction_reconcile_unreadable_record_total`.
- **S3 write-behind** (`upload/`) — `UploadManager` tokio task + bounded mpsc;
  SHA-256 integrity metadata; pending-upload log + replay for crash safety;
  separate flush vs. compaction upload managers. Pending-upload replay recognizes
  both legacy flat SSTable components and restored generation directories.
  Periodic sync publishes a generation only when all four required components
  (`Data.db`, `Partitions.db`, `Rows.db`, and `Filter.db`) are present; component
  presence is the invariant, so a valid zero-byte `Rows.db` is uploaded.
  **Wired into the flush path.**
- **Object-store backend** (`upload/config.rs`) — `ObjectStoreConfig` selects
  the durable backend. Default is S3-compatible (`AmazonS3Builder`, ETag CAS).
  Set `FERROSA_LOCAL_STORE_PATH` (or `[s3].local_path` in `ferrosa.toml`) to use
  a durable **local `file://` backend** (`object_store::LocalFileSystem`) for
  single-node durability without S3 — the previous "no object store" mode lost
  flushed SSTables silently. The local backend does **not** support conditional
  PUT (CAS); the startup probe (`probe_conditional_put_support`) detects this and
  manifest saves fall back to unconditional PUT. Last-writer-wins is correct
  because a single node is the only manifest writer.
  **Strict S3 (`FERROSA_S3_REQUIRED=true`):** without it, a missing
  `FERROSA_S3_ENDPOINT`/`FERROSA_S3_BUCKET` runs with local-only storage (now
  logged at WARN; it used to be silent), and a startup bucket check
  (`validate_object_store_access`: list + put + delete) that fails is a WARN plus a
  warning. With it, a missing or invalid S3 configuration, a local `file://`
  backend, or a failed bucket check stops startup with an error naming the switch.
  A value that is not a boolean is an error, not "off".
  **Request throttling (`upload/throttle.rs`):** every path shares one object
  store, wrapped in `ThrottledStore`. `FERROSA_S3_MAX_REQUESTS_PER_SECOND` paces
  requests evenly (no burst) and `FERROSA_S3_MAX_CONCURRENT_REQUESTS` caps them
  in flight (`object_store::limit::LimitStore`); unset means unlimited, and a
  non-positive or non-integer value stops startup naming the variable. A request
  answered `429 Too Many Requests` is retried with exponential backoff (10
  attempts, 250 ms doubling to 30 s) — `object_store` retries only 5xx — and the
  log reports the start and end of a throttling episode, not every 429. Set both
  when recovering from Cloudflare R2.
  **Downloads and pooling (`upload/download.rs`):** a component above
  `FERROSA_S3_DOWNLOAD_PART_BYTES` (16 MiB) is fetched as ranged GETs
  (`FERROSA_S3_DOWNLOAD_PART_CONCURRENCY`, default 4, through the same shared
  store) written with positional writes into a preallocated `.part` file, which is
  length-checked, fsynced and renamed; a failed part is retried alone (5
  attempts) and a 429 shrinks that object's concurrency. A smaller component is
  one GET through a 1 MiB buffered writer. Startup restore runs
  `FERROSA_RESTORE_CONCURRENCY` generations at a time, clearing each marker only
  once its generation is on disk. There is one client per process
  (`ObjectStoreConfig::client_options` holds the pool: `pool_max_idle_per_host`
  covers parts x restores + upload/delete workers, floor 32; 90 s idle timeout),
  and `object_store_and_config` errors rather than building a second client.
  **Stats (`upload/stats.rs`, `FERROSA_S3_STATS=1`):** a `StatsStore` layer under
  the throttle records per-operation counts, bytes, latency histograms, errors,
  429s and retries, and per (table, component) bytes, object-size histogram,
  ranged vs whole GETs, download throughput and read amplification. Exposed as
  `ferrosa_s3_*` Prometheus series and the virtual tables
  `system_observability.object_store_stats` and `object_store_ops`. Off by
  default; the keyed table is capped at 4096 keys with an overflow bucket.
- **Local cache** (`cache.rs`) — LRU eviction with manifest-pinned entries that
  are never evicted. With the local `file://` backend the cache is constructed
  durable (`new_with_durability`): the local disk *is* the store of record, so
  `evict_if_needed` is a no-op — evicting a flushed SSTable would drop its only
  durable copy.
- **Uploaded-SSTable cache eviction** (`enforce_uploaded_sstable_cache_limit`) —
  under disk pressure or over `local_cache_max_bytes`, deletes the local copy of
  manifest-listed SSTables of tables that are not hot, never-read tables first,
  then least recently read (`eviction_plan::order_for_eviction`, a pure
  function). A table is hot for `FERROSA_CACHE_HOT_WINDOW_SECS` (default 900,
  `0` disables) after a foreground read; the per-table stamp is set by the
  engine's point, range, index and full-text read entry points and never by
  anti-entropy repair, compaction or self-heal. When only hot tables keep the
  cache over its limit, one WARN names `hot_bytes` and the hot tables (and one
  INFO when it clears). Startup restore of evicted SSTables logs its plan and
  progress. Before deleting, it writes and fsyncs a
  `<gen>.evicted` marker; the engine constructors restore every marked generation
  from S3 before any table registers (`restore_evicted_sstables`), because
  generation discovery reads local files only. Only marked generations are
  restored: a manifest entry without a marker may be a compacted-away input, and
  restoring it would resurrect purged rows. Retiring a generation removes its
  marker: `delete_sstable_files` (truncate/eviction) and compaction's input
  retirement (`compaction::retire::retire`, which clears it last and fails the
  retirement if it cannot, FMEA ST-61). Retirement and S3 rehydration of one
  generation exclude each other (`generation_guard`), and a retired generation
  is not rehydrated again. A live reader that reopens a
  marked generation between eviction and restart rehydrates it from S3 first
  (`flush::rehydrate_if_evicted`, called by `open_file_sstable` and
  `open_sstable_from_dir`) and then clears the marker; an unmarked missing
  generation still fails to open so the read path's view-retry fires. System
  keyspaces are never evicted. See FMEA ST-38.
  **The marker records why** (`eviction_marker.rs`, FMEA ST-63): one JSON
  object `{version, trigger, source, written_at_unix_ms, generation_bytes,
  total_bytes, max_bytes, min_bytes, projected_available, target_free}`.
  `trigger` is `cache_cap`, `free_space`, `cache_cap_and_free_space`, or
  `recovered` (written by a tool, not an eviction decision); `source` names the
  writer (`ferrosa-storage evictor`, or `ferrosa-ctl sstable mark-evicted`). It
  is written to a temp file, fsynced, renamed and the directory fsynced before
  any component is deleted. Restore keys on the file's presence, never its
  content: an EMPTY file is a legacy marker (evicted, reason unknown), and a
  truncated or garbage file is also honoured as reason-unknown and reported
  (`MarkerState::{Recorded, Legacy, Unreadable}`; restore logs one census line
  and a WARN when any marker is unreadable). Unknown fields are ignored.
  The evictor sizes candidates from the files on disk (never
  `ManifestEntry::size`), counts each `(table, generation)` once, and logs one
  WARN (edge only) when the manifest claims >= 1.5x and >= 16 MiB more bytes
  than the same generations occupy on disk, or lists one twice (FMEA ST-64).
  **Eviction audit** (`eviction_audit.rs`, FMEA ST-65): every pass that finds
  pressure appends one JSON line to `<data_dir>/eviction-audit/audit.current.jsonl`
  (trigger, `max_bytes`/`min_bytes`/`target_free`/`projected_available`, the
  manifest's byte claim and the real on-disk total of the same set,
  duplicate entries, generations and bytes evicted, pid, build). The files are
  bounded by construction: `FERROSA_EVICTION_AUDIT_MAX_BYTES` (default 4 MiB,
  clamped 16 KiB..32 MiB; under 0.8% of the default 512 MiB free-space reserve)
  is split into 4 ring segments after a 4 KiB reserve, a record never lands in a
  segment it does not fit, and the oldest rotated segment is deleted before the
  current one is renamed, so no more than 4 files ever exist. Identical
  consecutive passes coalesce into one record with a count. Writing it never
  fails or delays an eviction: errors are reported on the edges (WARN when
  writes start failing, INFO when they recover) and counted. The latest pass is
  exposed as `ferrosa_storage_eviction_audit_*` metrics. Retention is the ring:
  the newest segments that fit the cap. `FERROSA_EVICTION_AUDIT_OFFLOAD=true`
  (off by default) uploads rotated segments to
  `<prefix>/eviction-audit/<instance>/<segment>` through the engine's shared
  throttled store, after the eviction, one segment per sync, one attempt with a
  timeout, and removes the local copy only after the put succeeds. A failed
  upload leaves the segment; the disk bound still wins, so the ring drops the
  oldest un-uploaded segment by age if uploads keep failing.
- **NVMe pinning** (`pin_config.rs`) — `PinMode::NvMe` keeps a table local and
  skips S3 upload; pin/unpin transitions reconcile the S3 lifecycle.
- **Secondary-index pipeline** (`index/`, `memtable/eager_index.rs`) —
  per-index state tracker, channel-based build scheduler, local/remote/off
  backends, FTI + vector (HNSW/IVFFlat) sidecars, artifact manifest.
  `LocalBackend` resolves SSTable components under the engine layout
  `<data_dir>/sstables/<keyspace>.<table>` and writes local sidecars beside the
  table's SSTables; legacy flat test layouts are still accepted. Remote build
  requests carry filtered predicates and clustering-column source metadata so
  remote sidecars match local builds. Existing SSTables and flush-time eager
  builds are marked pending in `IndexStateTracker` before async build
  submission, giving read planners a real completeness signal. Flush-time
  builds run only when the flush published an SSTable
  (`FlushOutcome::Published`); a flush with nothing to write touches neither
  the tracker nor the pin accounting (ST-43).
  Registrations are dogfooded to `system_schema.indexes`; `unregister_table`
  (the DROP TABLE choke point for every DDL route) cascades tombstones over the
  dropped table's registrations via `write_index_tombstones_for_table` and
  sweeps its tracker entries (t_ae06e925). `StorageEngine::drop_index` is the
  DROP INDEX choke point for live storage state: it removes the table store's
  memtable/vector metadata, sidecar read guards, and the tracker entry
  immediately, before restart; tracker cleanup is still idempotent when the
  table is not registered in this engine process.
  Because `unregister_table` also deletes the table's SSTable directory, cluster
  snapshot install must only reach it for explicit drops, not for table-map
  absence alone; the Raft state machine now enforces that guard before calling
  this storage cleanup primitive. Re-registering an already
  loaded table with index declarations merges any missing declarations into the
  existing store, keeping disk-loaded sidecars readable after local-schema
  boot preload. The registry-owned `schema.json` is a discriminated, bounded,
  crash-safe document; the engine's standalone recovery list is written to the
  separate `storage-schema.json`, so a flush can never replace registry state
  with an incompatible array. Concurrent registration is compare-and-install: a late schema
  replay merges declarations into the winning store instead of replacing its
  live memtable. Replaying an already-registered declaration is a no-op only
  when its column and index type agree; it preserves unflushed memtable
  postings (including phonetic postings) instead of replacing the live index.
  A newly registered scalar or vector index streams rows already present in the
  active or flushing memtable into its in-memory index before publication;
  CREATE INDEX therefore covers pre-existing unflushed rows without exposing
  an empty index to the query planner or collecting a temporary fallback scan.
  `update_schema` (the ALTER TABLE apply) remaps every positional index
  declaration through the old schema's column name, because adding a column
  that sorts before an indexed column shifts the indexed column's cell
  ordinal (ST-16); a declaration that cannot be mapped fails loud instead of
  silently indexing the wrong cell. It also flushes dirty pre-ALTER rows under
  the old schema before swapping layouts, so their SSTable serialization header
  preserves the write-time cell ordinals; reads and index backfills remap those
  physical ordinals through the per-SSTable header instead of interpreting old
  rows against the new layout.
  Boot-time
  `reload_indexes_from_system_schema(&PartitionKeyColumns)` returns an
  `IndexReloadOutcome` (`restored`/`skipped`); unresolvable rows emit one
  summary warn plus the `ferrosa_storage_index_reload_skipped_rows_total`
  counter (per-row detail at debug). Pre-existing orphans are never GC'd
  automatically — clean up with `DROP INDEX IF EXISTS`. The caller supplies
  each table's partition-key column names (the storage `TableSchema` records
  only the composite key type), so an index on a partition-key column — the
  tenant index of a `((tenant_id, session_id), ..)` table — is restored and
  backfilled through `add_partition_key_index` rather than skipped (ST-20).
  That resolution now lives in `register_index_in_engine(&TableId,
  IndexToRegister)`, which the reload calls per row and `ferrosa-cluster` calls
  when a replicated `CREATE INDEX` arrives. Before it was shared, only the CQL
  router built indexes, so every node except the one whose session ran the DDL
  had the index in schema and nothing in the engine (CL-18). It returns
  Vector registrations recover their dimension from the target column type and
  their HNSW/HVQ method from the persisted options. It returns `Ok(false)`, with
  a log line saying why, for a non-scalar index on a key column or a vector
  target whose declared type carries no dimension.
  A global index read (`read_by_index_each`) of an index the table does not
  declare returns an error naming the index, never zero rows: the planner
  chooses indexes from the CQL schema, so a consult of an undeclared index
  means schema and engine disagree, and an empty answer from one node is
  unioned by the coordinator into a short result (ST-20). The same read also
  refuses an index whose tracker is not `Current`: CREATE INDEX backfill and a
  failed sidecar publish may leave only a subset of postings available, and
  that subset must never be reported as a complete result. Callers can retry
  after the bounded background build finishes.
  Index postings are kept in row order — `(partition key, clustering)` — in
  every source: `MemtableIndex` inserts each key's postings sorted and unique,
  and sidecars are written in `(key, row)` order and re-sorted once at load,
  which normalizes files from the previous key-only writer without a format
  change. `read_by_index_each_after` k-way merges the sources
  (`OrderedPostings`), drops a row two sources hold by comparing it with the
  previous row, and resumes strictly after a cursor: memory is O(posting
  sources), never O(result); `read_by_index_stream_after` is its async form.
  A partition-key-column index is partition-granular (t_c5bccc65): every row
  of a partition shares the value, so the write path, the backfill and the
  eager builder post `(pk, [])` once per partition, and the walk streams that
  partition's rows in `rows_per_fragment` chunks through the retried
  `read_limited_rows[_from]` instead of one point read per row — one
  tenant's 101,848 entities were ~101k single-row reads. A cursor reopens its
  partition (the seek is inclusive at `(pk, [])`), and row postings left by
  sidecars written before this change are skipped once their partition has
  streamed whole.
  **Sidecars are memory-mapped (t_7ac6b0e3).** A scalar sidecar (format v2:
  sorted entries, an entry-offset table, a footer with a body CRC) is mapped,
  validated in one pass at open, and binary-searched in place; entries decode
  as borrowed `RowPositionRef`s, so a reader's heap does not grow with the
  file and mapped pages are reclaimable page cache (gauges
  `ferrosa_storage_index_sidecar_mapped_{bytes,files}`). Writers stream to a
  temp file, fsync and rename, so a mapped file is never truncated; v1 files
  are converted at open through the spilling `ExternalSorter`. Flush maps the
  sidecars it writes; compaction installs its output's sidecars by k-way
  merging the inputs' (postings are keys, so they stay valid); the index
  scheduler installs each backfilled sidecar before marking the SSTable
  indexed. Restore pulls every index artifact of a generation from S3
  completely before publishing it, and the S3 sync uploads sidecars built
  after their generation was already in the manifest.
- **Index rebuild coverage** (`rebuild_index`, `index::orphan`, FMEA ST-53) —
  a backfill walks the store's live SSTable set and classifies each generation
  by its on-disk state, never by error text: `Data.db` present is built; an
  eviction marker is rehydrated and built (restore failure fails it); `Data.db`
  gone with the TOC surviving is `Vanished` (compacted away, discounted); a
  generation with no files at all is `Failed` (stale enumeration). Completeness
  needs zero failures AND every enumerated SSTable accounted for, a sidecar that
  cannot be written, reopened or installed is a failure, and any failure leaves
  the tracker stale so index reads are refused. `RebuildOutcome` reports
  `sstables_failed`, and `add_partition_key_index` returns the same
  `RebuildOutcome` (an incomplete backfill is a stale index, not an `Err`; the
  CQL router and the startup reload log it at ERROR). A metadata-only
  generation (TOC, no `Data.db`) is still discounted on the inference that
  compaction removed it, but each one logs a WARN naming the generation and
  calling the claim unverified; nothing records a retirement yet.
  **Row-count reconciliation** (`index::orphan::ScanTally`, run inside
  `LocalBackend::build`): the build compares the partitions it read with the
  partition count the writer recorded in the SSTable footer, and, for a
  partition-key index, the entries it produced with the partitions it owed one
  to. Tolerance is zero and the cost is nil (counters on the pass the build
  already makes). A mismatch fails the build, so the index stays stale. Cell and
  clustering indexes get only the partition-walk check, because a null cell
  legitimately yields no entry; the remote backend is not reconciled. An
  entry-level check for those kinds would need a full table scan and is not done.
  `ferrosa-ctl index rebuild` prints the failed count and exits non-zero when a
  rebuild did not complete.
- **Full-text search** (`fulltext_search(table, index, query, limit)`) —
  searches the memtable FTI + the `-FTI-{index}.db` sidecar of each **live**
  SSTable, found from the store view (`TableStore::fulltext_live_sidecars`),
  never from a directory listing: compaction leaves its inputs' index
  artifacts on disk, and reading those returned keys for superseded rows and
  made every query's cost grow with the table's whole compaction history.
  **Compaction builds the output's FTI sidecar before the swap**, and a query
  that finds a live SSTable without one builds and persists it first
  (`plan_missing_fulltext_sidecars` → `FulltextSidecarBuild::run`: planned
  under the table lock, run without it, single-flight per table, atomic
  temp-file + rename). Only an SSTable whose sidecar cannot be built (logged
  at ERROR) or a non-persisting target falls back to scanning it on every
  query, so a stable row is never dropped from `fts_match` (BUG-F-007 /
  t_0455c0a1). Before this, compaction wrote no sidecar and the fallback ran
  on every query for the life of every compacted SSTable — 7–13 s per replica
  on a live cluster, past the coordinator's 3 s Bulk-lane budget (FMEA ST-58). Memory is
  bounded (t_ee98faa0 layer 2 — a broad `fts_match` used to OOM every
  replica): `limit` is the QUERY-derived `LIMIT k` pushed down by the
  coordinator (never a server cap) and bounds every per-source working set to
  a top-k; single-term queries stream postings straight off the sidecar file
  (`ferrosa_index::fulltext::stream`) without reading or deserializing the
  whole index; transient memtable/fallback FTIs are queried in place (no
  serialize→deserialize round trip) and built per-SSTable, not across all
  uncovered SSTables at once. Only the queried index's sidecars are consulted
  — orphaned registrations are never touched on the query path. Guarded by
  `tests/fulltext_replica_memory_bound.rs` (allocator-tracked peak: O(k),
  independent of matching-doc count),
  `engine::tests::fts_search_touches_only_queried_index_sidecars`,
  `fts_search_ignores_sidecars_of_generations_that_are_not_live`,
  `fts_after_compaction_uses_a_sidecar_and_sees_only_current_rows` and
  `fts_sidecar_less_live_sstable_is_tokenized_once_not_per_query`.
  The no-`LIMIT` shape has a streaming twin, **`fulltext_search_each(table,
  index, query, on_hit)`** (t_4ae47a9f layer 2b): single-term walks hand each
  matching doc key to the callback with an O(1) working set (no score map;
  `ControlFlow::Break` = consumer-paced early exit), so replica memory is
  independent of the match count even without a LIMIT — the shape that
  OOM-killed nodes in t_8fc24ce2. Keys arrive unordered and may repeat across
  sources (caller dedups); compound queries delegate to `fulltext_search`
  internally. Guarded by `tests/fulltext_streaming_each_memory_bound.rs`.
- **Snapshot / PITR** (`snapshot/`, `restore/`, `commitlog/archiver.rs`) —
  S3 snapshot manager, commit-log archiving, restore manager with validation.
- **Restore on boot** (`restore/intent.rs`) — a node started with
  `FERROSA_RESTORE_SNAPSHOT` set opens by restoring that snapshot instead of
  taking the ordinary open path. `FERROSA_RESTORE_POINT_IN_TIME` (RFC 3339 UTC)
  sets the commit-log replay cutoff; `FERROSA_RESTORE_FORCE=1` accepts a
  snapshot taken by a different node.

  The intent is applied **at most once**. An env var survives a reboot, so
  after a successful restore the node records a fingerprint of the intent in
  `{data_dir}/.restore-applied` and later boots carrying the same intent skip
  it — otherwise every restart would silently roll the database back and
  discard everything written since. A different snapshot or cutoff is a new
  request and is applied normally. The marker is written only after the engine
  opens, so a failed restore is never recorded as done.

  Timestamp parsing is deliberately strict (`parse_rfc3339_micros`): non-UTC,
  unpadded, and space-separated forms are rejected rather than coerced, and a
  cutoff with no snapshot is an error. A silently mis-parsed cutoff would
  restore to the wrong moment, which is worse than refusing. Validation happens
  at startup, before any SSTable is downloaded.

  > **The HTTP endpoint is not wired to this.** `POST /api/restore` validates a
  > request and replies `202 "restart the node to complete restore"`, but it
  > persists no intent, so restarting after calling it does **not** restore.
  > The env-var path above is the only one that works today.
- **Quarantine + self-heal** (`quarantine.rs`, `self_heal/`) — malformed rows
  found at flush/replay are written to a durable `quarantine/*.jsonl` sidecar
  instead of crashing; the self-heal controller detects corrupt SSTables and
  quarantines them under a safety rail.
- **Replay without a schema degrades instead of exiting** (`replay_set_aside.rs`,
  FMEA ST-52) — when no `schema.json`/`storage-schema.json` is usable, replay
  buffers up to `FERROSA_MAX_PENDING_REPLAY_WITHOUT_SCHEMA` mutations in memory
  and appends the overflow to `<data_dir>/commitlog-unreplayed/*.unreplayed`
  (CRC-framed, fsynced before the commit-log segment is deleted). The engine
  opens, logs table ids/count/path at ERROR, bumps
  `ferrosa_commitlog_replay_set_aside_mutations_total` and reports the unapplied
  remainder through `StorageEngine::replay_set_aside_status()`, the
  `ferrosa_commitlog_replay_set_aside_pending_mutations` gauge, and `/readyz`
  (503, `waiting_for: "set_aside_mutations"`). Set-aside mutations are durable
  but NOT visible to reads until re-ingested.
- **Set-aside re-ingest** (`replay_set_aside.rs`, `StorageEngine::finish_construction`)
  — every constructor finds `*.unreplayed` files: `StorageEngine` holds a
  `SetAsideLedger` whose only constructor scans the data dir, so a constructor
  cannot build the struct without adopting (and ends with `finish_construction`,
  which re-ingests tables registered before the engine existed, as `open`'s are).
  `every_public_constructor_adopts_a_pre_existing_set_aside_file` lists them all.
  Each table registration (startup local schema, the
  `open` schema, or DDL) re-ingests that table's frames. Frames stream one at a
  time; each is applied through a strict path (a failed row aborts the file and
  keeps it whole), the touched tables are flushed to SSTables, and only then
  does the file shrink: frames for tables still unknown are copied to
  `<file>.partial`, which atomically replaces the file, and a fully applied file
  is removed. A crash at any point leaves the original, and re-applying a frame
  rewrites identical cells, so a repeat adds no rows. Frames of a table that
  was DROPPED after they were written are never applied to a re-created table of
  the same name: `unregister_table` durably records each drop in
  `<data_dir>/dropped-tables.json` (`table_drops.rs`; a drop that cannot be
  recorded is refused), and a file created at or before a table's drop holds only
  earlier-incarnation frames. Those are copied (fsynced) to
  `<data_dir>/commitlog-quarantine/<file>.stale`, logged at ERROR, counted in
  `replay_set_aside_stale_frames_total`, and only then leave the set-aside file.
  The engine has no stable table id (the registry's UUID never reaches storage),
  so a file created AFTER the drop that still holds pre-drop frames is not
  detected. A torn or corrupt file is
  never skipped or partly applied: it is reported (ERROR, `/readyz`
  `unreadable_files`) and kept for the operator. Offline route:
  `ferrosa-ctl commitlog set-aside <data-dir> [--apply]`
  (`StorageEngine::reingest_set_aside_offline`). Replay also
  expands legacy whole-value collection cells into element cells so the SSTable
  writer's mixed-cell assertion cannot fire at the next flush.
  Mutations for tables absent while a schema exists are held in memory up to
  `FERROSA_MAX_DEFERRED_REPLAY_MUTATIONS` (default 10000; invalid values fail
  `open` naming the variable) and the overflow goes to the same set-aside file,
  counted per table and logged at ERROR (FMEA ST-62).
- **Range reads fail loud on an unreadable SSTable** (`store.rs`,
  `with_retried_scan`, FMEA ST-41) — `read_range*`, `read_token_range[_bounded]`,
  `walk_token_range[_for_digest]`, the time-series cursor and the full-text
  sidecar-less scan never return a partial `Ok` when an SSTable in their view
  cannot be opened or read (e.g. an evicted file whose S3 rehydrate failed).
  They retry against a fresh view (compaction retired the input), then
  quarantine the SSTable and return a typed `Error::CorruptSstable`. Quarantined
  SSTables are not skipped by range reads: their rows are still missing.
  A decode error AFTER the SSTable opened (mid-stream, in `walk_token_range[_for_digest]`
  or the bounded-merge cascade) is attributed to the `MergeReader` that raised
  it and takes the same path; a failure after the first row was delivered is
  final (a retry would deliver twice), and an error from the caller's own row
  callback is never mistaken for an SSTable failure. A generation whose open
  failed is remembered for a short TTL (5 s, at most 256 entries — the
  `MissingSstableCache` negative cache), so later reads and the eight retries
  fail fast without reopening; it still returns the typed error, never a short
  `Ok`. The entry is dropped when the generation is seeded (restored or
  rewritten), when `resolve_sstable_quarantine` is called, or when it expires.
  Counters: `missing_sstable_open_failures`, `missing_sstable_fast_fails`.
- **Startup SSTable health** (`sstable_health.rs`) — decides whether a
  generation on disk can serve reads before it is loaded. A critical component
  (`Data.db`, `Partitions.db`) that is **missing**, **zero-byte**, or
  **unreadable** withholds the generation; `Rows.db` is excluded because the
  writer legitimately emits it empty for simple partitions. The judgement is a
  pure function so it is testable without a disk or a cluster, and the log line
  names the component and the reason — a manifest entry pointing at a file that
  is gone used to surface only as `No such file or directory (os error 2)` with
  no table, generation or path, failing every read of that table.
- **Read fanout detection** — every partition-read attempt exports
  `ferrosa_storage_read_sstable_fanout_max`; reads above 32 immutable
  descriptors increment `ferrosa_storage_read_sstable_high_fanout_total` and
  emit a process-rate-limited ERROR naming the table and reader-pool capacity.
  The read still streams through the bounded pool; diagnostics never open an
  extra reader or materialize a table.
- **Accord** (`accord/`) — per-shard conflict index + protocol log for
  strict-serializable transactions. Also defines `TransactionCommitter` (ADR-021):
  the front-end-facing seam CQL/Postgres `BEGIN`/`COMMIT` call to commit a
  buffered multi-key write-set; the ferrosa-cluster Accord impl resolves replicas
  and drives the multi-key transaction. `MockTransactionCommitter` backs
  front-end unit tests without a cluster.
- **Time-series** (`timeseries/`) — ring-buffer aggregation, late-data handling,
  WASM aggregate execution, materialization queues.
- **Virtual tables / observability** (`virtual_tables.rs`, `metrics.rs`) —
  `system_observability.storage_stats`, `system_views.secondary_indexes`.
- **RAM budget + spill threshold** (`spill_budget.rs`) — detects the process
  memory budget (cgroup v2 `memory.max` → cgroup v1 `memory.limit_in_bytes` →
  `/proc/meminfo` `MemTotal` → 1 GiB floor; `"max"`/near-`i64::MAX` sentinels are
  treated as unlimited). Detection is injectable (`BudgetSources`) and cached; the
  spill threshold defaults to 50% of the budget, tunable via
  `FERROSA_RANGE_SPILL_THRESHOLD_PCT` / `FERROSA_RANGE_SPILL_THRESHOLD_BYTES`
  (`process_spill_threshold_bytes`).
- **External merge sort** (`external_sort.rs`) — bounded-memory spilling sort of
  CQL result rows (`Vec<Option<CqlValue>>`) for the unbounded `ORDER BY` (no
  `LIMIT`) shape. `ExternalSorter` moves rows into a buffer, spills sorted runs to
  disk (length-prefixed serde_json) once the threshold is crossed, then
  cascade-merges runs in fixed fan-in passes (`MERGE_FANIN = 64`) into a final
  bounded k-way merge (`SortedRows`, `RowOrder`). Peak working set is
  `O(MERGE_FANIN)` — independent of the row count. Spill/merge I/O errors fail
  loud; runs live under the `TempSortTableReservation` dir (cleaned up on drop).
- **Range merger run grouping** (`range_merger.rs`) — to keep the merge heap
  small, token-disjoint SSTables are grouped into concatenated "runs"
  (`partition_into_disjoint_runs`), one heap source per run instead of one per
  SSTable. **Correctness invariant:** a run may only concatenate SSTables in
  which every partition key appears at most once, else the run re-emits a shared
  key and the heap — seeing one source per run — never routes the duplicates
  through `merge::merge_partitions`, double-counting rows (the `COUNT(*)`
  over-count on `agent_memory.typed_edges`, FMEA ST-13). Disjointness is proven
  by **decoding** each SSTable's partition-index bounds to a `DecoratedKey` and
  coloring intervals in token order; SSTables whose bounds are not
  byte-comparable (older/Cassandra-shaped encodings that fail
  `byte_comparable::decode`) are each isolated into a singleton run so they stay
  independent heap sources. `count_range` (COUNT(*)) and the row-scan paths share
  this merger, so both dedup identically. The streaming fragment path
  (`emit_fragment`) additionally **fails loud** if a source yields rows out of
  clustering order within a partition (a legacy/corrupt SSTable) rather than
  serving a silent partial — compaction's legacy-format rewrite is the at-rest
  fix.

## Public API (key entry points)

| Area | Items |
|------|-------|
| Engine | `StorageEngine`, `StorageEngineConfig`, `new`/`open`, `register_table[_with_indexes]`, `add_index[_with_predicate]`, `add_clustering_index` (clustering-column indexes, t_430c4188), `shutdown` |
| Write | `write`, `batch_write`, `write_atomic_batch`, `apply_batch`, `begin_batch`/`BatchTxn`/`BatchOp`, `replay_mutations` |
| Read | `read`, `read_range`, `read_token_range[_bounded]`, `range_iter[_projected|_fragmented]`, `count_range`, streaming `read_by_index_each`/`read_by_index_stream` (global lookups visit postings incrementally), `read_by_index_in_partition` (keyed consult restricted to one partition and fail-loud bounded), `ann_search`, `fulltext_search`, `walk_token_range[_for_digest]` |
| Maintenance | `flush`, `flush_if_needed`, `flush_all`, `poll_compactions`, `truncate`, `sync_sstables_to_s3` |
| Snapshot/PITR | `create_snapshot_with_store`, `open_from_snapshot_with_store`, `open_from_snapshot` (builds the object store from `config.object_store`; the restore-on-boot entry point), `list/delete_snapshot_with_store` |
| Restore intent | `restore::RestoreIntent` (`from_env`, `from_vars`, `point_in_time_micros`, `already_applied`, `mark_applied`), `restore::parse_rfc3339_micros`, `ENV_RESTORE_SNAPSHOT` / `ENV_RESTORE_POINT_IN_TIME` / `ENV_RESTORE_FORCE` |
| Abstraction | `DataStore` / `LocalDataStore` (the `Arc<dyn DataStore>` boundary) |
| Spill/sort | `ExternalSorter`, `RowOrder`, `SortedRows`, `spill_budget::process_spill_threshold_bytes`, `reserve_order_by_temp_sort_table`/`TempSortTableReservation` |
| Config types | `CommitLogConfig`, `SyncStrategyConfig`, `CompactionConfig`, `ObjectStoreConfig`, `Mutation`, `TableId` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cdc`** — `CdcBus` for change-data-capture emission on the write path.
- **`ferrosa-common`** — `Token`, `DecoratedKey`, `PartitionKey`, `CellValue`,
  `TableSchema`, `Result`/`Error`, cell/clustering validation.
- **`ferrosa-index`** — secondary index builders (BTree/Hash/FullText/Vector),
  FTI sidecar merge.
- **`ferrosa-row-bridge`** — `decode_clustering` for clustering-column
  secondary indexes (t_430c4188): the write path and sidecar builds split a
  row's composite clustering-key bytes to extract the indexed component.
- **`ferrosa-schema`** — table/keyspace metadata and system schema persistence.
- **`ferrosa-sstable`** — `Partition`, `Row`, `SSTableReader`/`SSTableWriter`,
  BTI format I/O.

External: `object_store` (aws), `tokio`, `arc-swap`, `parking_lot`,
`crossbeam-skiplist`, `crossbeam-channel` (T-021: `CancelToken`'s channel, the
compaction executor's task/result queues), `crc32fast`, `sha2`, `dashmap`,
`serde`, `bytes`, `fs2`.

**Called by** (crates that depend on this):

- **`ferrosa`** (main binary), **`ferrosa-cluster`**, **`ferrosa-cql`**,
  **`ferrosa-ctl`**, **`ferrosa-graph`**, **`ferrosa-index-builder`**,
  **`ferrosa-loadgen`**, **`ferrosa-postgres`**, **`ferrosa-session`**,
  **`ferrosa-sparql`**.

**Remote index builder client**: `index::RemoteBackend` authenticates to
`ferrosa-index-builder` with `Authorization: Bearer $FERROSA_INDEX_BUILDER_TOKEN`
(logged loudly when unset, since the builder then refuses and every build falls
back to local), sends an engine-issued `job_id` instead of an S3 prefix, and
rejects an implausible `sidecar_s3_path` in the response.

## Tests

~1024 test functions across in-module `#[test]`/`#[tokio::test]` and 17
integration files (`tests/`), including proptest property suites
(`engine_property`, `compaction_property`, `commitlog_property`,
`property_tests`) and the repair fuzz harness (`repair_fuzz.rs`, gated behind
`test-generators`/`fuzz-fileio`). Live-infra tests are behind the
`live-infra-tests` feature + `FERROSA_TEST_*` env vars. No `#[ignore]`.

Slow tests (compaction soak across many seeds, high-volume ingest, concurrent
write+flush, the 2k-mutation commit-log replay PITR case) live in a `mod slow`
gated behind the `slow-tests` feature instead of `#[ignore]`. PR CI compiles
them (`--all-features`) but skips running them (`--skip ::slow::`);
`nightly-slow-tests.yml` is where they run. Run one locally with:

```bash
cargo test -p ferrosa-storage --features slow-tests -- ::slow::e4_slow_pitr_commit_log_replay_1k_plus_1k
```

The read-vs-compaction race stress (`race-stress` feature,
`read_compaction_race_stress`) has **no CI job**. It ran nightly on a throwaway
shared-cpu Fly machine until that job was removed for leaking a Fly app per run
(the runner cancels the step at the job timeout, killing the script before its
cleanup trap can destroy the app). Run it by hand on a CPU-starved host:

```bash
cargo test -p ferrosa-storage --features race-stress --release \
  read_compaction_race_stress -- --nocapture
```

Scale with `RACE_KEYS` / `RACE_READERS` / `RACE_SECS` / `RACE_FLUSH_EVERY`.

### Compaction cancel-safety and crash recovery (T-020–T-026)

The T-023 retirement crash seam is scoped to the explicitly injected Tokio
poll task. Concurrent tests and startup reconciliation do not inherit the
fault; unwinding or dropping the scoped future removes the injection.
The digest-corruption test passes a per-task structural verification policy;
it never disables the verification checkpoint for concurrent cancellation tests.

`src/compaction/cancel_harness.rs` names every step in the compaction
lifecycle (`CancelPoint`) and gives production code a `cancel_point!(...)`
hook to call at each one, behind `cfg(any(test, feature = "test-support"))`
(a no-op, compiled to nothing, otherwise). `src/compaction/cancel_oracle.rs`
is a plain model of every acknowledged write (`WriteOracle`) plus
`assert_cancel_invariants`, which checks I1 (content matches the oracle), I2
(exactly one of {inputs, output} discoverable after startup reconciliation
runs), I3 (every discoverable generation opens and walks), and I4 (no
`.promote-*`/`.retired-*`/stale-`.tmp`/staging leaks). See
`specs/sstable-write-pump/compaction-cancel-safety.md`.

**T-021 makes cancellation real**, unconditionally (in every build, not only
tests): `try_submit` creates a `CancelToken`, and every merge-time
`cancel_point!` call site is now paired with a real `token.check()` that
returns `Err` and rolls back whatever this task staged so far; `engine.rs`'s
`poll_compactions` adds one more real check immediately before promoting (the
last free cancel point). The harness now also registers each task's live
token by table-id scope (`cancel_harness::register_cancel_token` /
`cancel_now`), so a test's cancel-point hook can genuinely cancel — not
merely record having reached — a `CancelPoint`:

- `cancel_harness_*` (`cancel_harness_integration.rs`): the hook fires at
  every point during a real, uncancelled compaction, and the oracle +
  invariant checker agree on a clean flushed table.
- `cancel_token_*` (`cancel_token_tests.rs`, T-021): CS1 — cancelling at
  every honoured checkpoint (`InputOpen` through `BeforePromote`) rolls
  back with no restart needed, and I1-I4 still hold after one; CS3 — the
  cancel-to-`Err` latency is small and `compaction_cancel_latency_seconds`
  records it; CS4 — `CompactionExecutor::shutdown()` cancels a task stuck
  mid-merge promptly instead of waiting for it to finish; CS14 (folded into
  each CS1 case) — the same inputs compact again afterward with identical
  content; CD1 — a worker parked on an empty task channel exits shutdown
  immediately (crossbeam `select!`, no `recv_timeout` poll interval).
- `cancel_crash_sweep_*` (`cancel_crash_sweep_tests.rs`, CS2 in
  `test-specification.md` L10): a crash-twin subprocess harness, unrelated to
  T-021's live-process cancellation. Each test re-execs the same test binary
  filtered to itself, the child installs a hook that `std::process::abort()`s
  (SIGABRT) at one `CancelPoint`, drives a real compaction into it
  (compressed and uncompressed), and the parent asserts the child died by
  signal, reopens a fresh engine on the same data dir, and checks I1-I4.

Points strictly before the C2 commit point (input open through
`BeforePromote`) roll back cleanly, because `compaction/<table>/` staging is
unconditionally wiped at every engine open. Every point at or after the
commit (`AfterPromote` through input retirement) rolls forward cleanly onto
the promoted output: T-022 writes the durable replacement record with the
real, already-reserved output generation id before promotion runs (forge
t_cb6fa288 — `StorageEngine::reserve_compaction_promotion_target`), and
T-023's startup reconciliation retires every input the record lists,
unconditionally and idempotently, regardless of how far retirement got
before the crash. T-026 keeps the replacement record authoritative through
S3 upload, manifest publication, and input-delete enqueue. Startup rebuilds
missing upload work from the record, replays the pending-upload ledger, and
retries deletion for manifested records. Pinned and local-only compactions
finish local retirement without publishing an S3 manifest. The mock-store
crash sweep covers pending-log, manifest-CAS, delete-enqueue, and pinned
retirement interruption. All `cancel_crash_sweep_*` cases therefore run
unconditionally today; there is no `known-open-window`-gated case (the
feature that used to exist under that name was deleted 2026-09-26 once
nothing gated on it):

```bash
cargo test -p ferrosa-storage cancel_harness_
cargo test -p ferrosa-storage cancel_token_
cargo test -p ferrosa-storage cancel_crash_sweep_
cargo test -p ferrosa-common cancel
```


Unix-only (signal-based crash detection). The `RetireInput` `CancelPoint`
fires once per whole input generation, not per component file, so it cannot
exercise "one generation half-deleted mid-component-loop" (some of a single
generation's own component files gone, some not) — the remaining scope of
T-024 (C4: atomic per-generation retirement) needs a finer, per-component
hook. See [Roadmap](specs/roadmap.md).

### CQL write pressure admission

Async CQL writes use per-table pressure `max(active memtable / hard limit,
write-pump blocked-time rate)`. At pressure 0.7 they request a background flush
and wait on that table's `Notify` for a pressure-scaled deadline; they re-check
once and reject at pressure 1.0. Synchronous storage callers keep the hard
admission check. Table pressure is updated atomically; metrics registration and
scraping use a registry lock only off the write path.

Runtime settings are read when the storage engine starts:

| Variable | Default | Accepted range | Invalid value |
|---|---:|---:|---|
| `FERROSA_WRITE_SOFT_PRESSURE_THRESHOLD` | `0.7` | `0.05..=0.95` | ERROR log, use `0.7` |
| `FERROSA_WRITE_SOFT_DELAY_MAX_MS` | `50` | `1..=1000` | ERROR log, use `50` |

The pressure threshold is a ratio; the maximum delay caps the grace period.
The pump blocked-time rate is clamped to `[0, 1]`. Invalid values never prevent
startup. Prometheus exports `ferrosa_storage_write_admission_delayed_total`,
`ferrosa_storage_write_admission_delay_seconds`,
`ferrosa_storage_write_admission_rejected_total{reason}`, and the per-table
`ferrosa_storage_write_pressure_ratio` gauge.

### Flush and compaction runtime tuning

The storage engine reads these bounded `FERROSA_*` settings once, on first
flush/compaction runtime configuration. Missing values keep the defaults shown
below. Malformed, unreadable, or out-of-range values log at `ERROR` and use the
default. Values are process-wide for the lifetime of the engine.

| Setting | Default | Accepted range | Effect |
|---|---:|---:|---|
| `FERROSA_MAX_AUTOMATIC_FLUSHES_PER_POLL` | 8 | 1–1,024 | Maximum automatic flushes started per maintenance poll. |
| `FERROSA_MAX_COMPACTION_INPUTS_PER_TASK` | 64 | 2–256 | Ceiling applied to `FERROSA_COMPACTION_MAX_THRESHOLD`; bounds fan-in independently of the input-byte limit. |
| `FERROSA_MAX_SCHEDULED_TABLES_PER_POLL` | 8 | 1–1,024 | Maximum tables that receive a compaction task per maintenance poll. |
| `FERROSA_MAX_RESULTS_PER_MAINTENANCE_POLL` | 8 | 1–1,024 | Maximum compaction failures and completed results drained per poll. |
| `FERROSA_MAX_AGE_FLUSH_FLOOR_BYTES` | 16 MiB | 1 byte–1 TiB | Upper bound for the minimum memtable volume required for age-triggered flushes. |
| `FERROSA_PER_COMPACTION_MEM_BUDGET_BYTES` | 256 MiB | 1 byte–1 TiB | Per-task memory estimate used to derive concurrency from the node memory limit. |
| `FERROSA_MAX_AUTO_COMPACTION_PARALLELISM` | 8 | 1–64 | Caps auto-derived concurrency and workers, and bounds explicit compaction worker/concurrency settings. |
| `FERROSA_COMPACTION_WORKERS` | auto-derived from CPU count | 1–configured compaction ceiling | Explicit compaction worker count. |
| `FERROSA_MAX_CONCURRENT_COMPACTIONS` | auto-derived from CPU and memory | 1–configured compaction ceiling | Explicit concurrent merge cap. |
| `FERROSA_MAX_FLUSH_PARALLELISM` | 64 | 1–256 | Caps `FERROSA_FLUSH_PARALLELISM` and the shared flush pool width. |
| `FERROSA_FLUSH_PARALLELISM` | host CPU count | 1–configured flush ceiling | Shared flush/fsync pool width. |
| `FERROSA_DIGEST_READ_CHUNK_BYTES` | 1 MiB | 1 byte–64 MiB | Reused buffer size for staged SSTable digest verification. |
| `FERROSA_COMPACTION_TASK_QUEUE_CAPACITY_PER_WORKER` | 1 | 1–32 | Bounded queued tasks per compaction worker. |
| `FERROSA_COMPACTION_RESULT_QUEUE_CAPACITY_PER_WORKER` | 2 | 1–32 | Bounded completed results and failures per compaction worker. |

Values outside their documented ranges, including unreadable environment
values, log at `ERROR` and use the corresponding default. These practical
ceilings keep per-poll work, thread counts, queue allocation, and digest buffers
bounded while allowing operators to tune within the supported range. The
configured compaction parallelism ceiling caps both auto-derived and explicit
worker/concurrency settings, while queue capacities remain bounded per worker.
Raising values trades more throughput headroom for greater memory and I/O pressure.
If Rayon cannot create the requested flush pool, initialization logs `ERROR` and
retries with one worker; if that also fails, engine initialization returns an
error instead of panicking.
The age-flush floor trades earlier WAL retention relief for the risk of creating
more small SSTables. Digest chunk size changes verification read granularity;
digest calculation and staged-output verification remain mandatory.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — write path and read path (mermaid)
- [FMEA / known issues](specs/fmea.md) — failure modes ranked by RPN
- [Roadmap](specs/roadmap.md) — Now / Next / Later

### Pump wiring acceptance (T-045)

File-backed sharded flushes stream each shard through the aligned pump into an owned staging directory, retain only component manifests, and publish the complete reader set in one view update. Guards remove unfinished staging after workers join. Wiring acceptance covers compressed/plain flush, compaction, restart, runtime pump settings, exact component bytes, and digest readback.

Internal flush and compaction writers defer component sync to the
`FileFlushTarget` staged handoff. The target syncs every staged component,
verifies digests and structure, applies the buffered cache hint after readback,
then promotes files and syncs the directory. Standalone SSTable callers keep
the normal durable finish path.

Shard workers collect moved component manifests directly into the fallible output vector; there is no intermediate vector of per-shard results.

### Backpressure verification (T-041)

Real engine tests gate flush/compaction output and digest readback by unique
temporary directory. They assert active-memtable admission reaches a plateau,
rejections do not grow it, accepted rows remain readable, a released flush can
complete while compaction stays gated, and WAL/input retirement waits for digest
verification. Shutdown cancellation is observed before releasing the controlled
device call; no cancelled output replaces the live inputs. This explicitly
accounts for the fact that cancellation cannot interrupt an arbitrary syscall.

`FERROSA_COMPACTION_READAHEAD_BYTES` defaults to 1 MiB, accepts 1 byte through
256 MiB, and rounds to a 4096-byte block. Invalid values log ERROR and use the
default; changed alignment logs configured/effective values at WARN. No startup
failure is introduced for a malformed value.

Compaction retires local inputs through durable `.retired-<generation>` renames.
Flat layouts hide Data.db first and include secondary, full-text and vector
sidecars; generation directories move atomically. Failures emit WARN, increment
`ferrosa_storage_compaction_retire_failures_total`, and retain the replacement
intent so startup reconciliation can retry. Component names stream from the
directory without collecting file contents or a component inventory.

Completed compactions wait on the bounded result sender together with cancellation
and shutdown channels. Cancellation while that queue is full wakes the worker
without a sleep interval, removes its unpromoted staging components and releases
input claims. Result accounting increments before publication so an immediate
consumer cannot underflow the pending-result counter.

When write admission observes the disk reserve exhausted, the executor cancels
the registered task with the largest total input size. One disk-pressure
cancellation remains outstanding until that task releases its claim, preventing
a burst of rejected writes from cancelling all tasks. Admission stays closed
until the existing free-space check observes recovery. The cancellation registry
now keeps one record per task instead of duplicating its token for every input.

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

Operator stop requests use `CancelReason::Operator` under the task registry lock.
They select all current tasks or one registered table, preserve the first recorded
cancellation reason, and leave admission enabled. Counts describe the registry at
request time; the caller does not wait for finalization or promise reclaimed disk.

### Compaction digest-failure retry

Output digest or verification failures retry with exponential backoff, starting
at 1 s and capped at 60 s by default. Configure
`FERROSA_COMPACTION_RETRY_BACKOFF_INITIAL_MS` (1–60,000 ms; default 1,000) and
`FERROSA_COMPACTION_RETRY_BACKOFF_MAX_MS` (1–600,000 ms; default 60,000).
Out-of-range values are clamped with a WARN; invalid integers use the default
with an ERROR. If the maximum is below the initial delay, it is raised to the
initial delay with a WARN.

After three consecutive failures for a table, compaction pauses by default.
`FERROSA_COMPACTION_DIGEST_FAILURE_LIMIT` sets the pause threshold (1–100;
default 3; invalid values use the default). The in-memory pause clears on
restart, or an operator can call
`StorageEngine::resume_table_compactions_after_digest_failures(table_id)` to
clear the pause and retry streak; the next failure starts at the initial delay.

## jsonb (T-150)

`external_sort` payload accounting counts the canonical jsonb bytes (T-150); a document-sized value never accounts as 0.
