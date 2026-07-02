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
  Per-partition merge-on-write (cell-level LWW, tombstone merge).
- **Commit log** (`commitlog/`) — segmented WAL with CAS-based lock-free
  allocation, forward-linked sync markers, crash-recovery replay, CDC reader,
  S3 archiver for PITR, and per-table checkpoints. Three sync strategies
  (`Batch`, `Periodic`, `Group`); **default is `Periodic`** → a bounded
  durability window (see FMEA).
- **Flush** (`flush.rs`, `store.rs`) — `TableStore` composes active/flushing
  memtables + SSTable descriptors behind a single `ArcSwap<StoreView>`. Flush is
  serialized by a per-table `Mutex`; reads/writes are never blocked. Optional
  `write_verify` self-readback after every flush.
- **Compaction** (`compaction/`) — `CompactionExecutor` on dedicated
  `std::thread` workers behind a global `CompactionGate`. STCS (default) and UCS
  (CEP-26 density-based) strategies. A compaction validator (oracle + differential
  checks) gates correctness.
- **S3 write-behind** (`upload/`) — `UploadManager` tokio task + bounded mpsc;
  SHA-256 integrity metadata; pending-upload log + replay for crash safety;
  separate flush vs. compaction upload managers. Pending-upload replay recognizes
  both legacy flat SSTable components and restored generation directories.
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
- **Local cache** (`cache.rs`) — LRU eviction with manifest-pinned entries that
  are never evicted. With the local `file://` backend the cache is constructed
  durable (`new_with_durability`): the local disk *is* the store of record, so
  `evict_if_needed` is a no-op — evicting a flushed SSTable would drop its only
  durable copy.
- **NVMe pinning** (`pin_config.rs`) — `PinMode::NvMe` keeps a table local and
  skips S3 upload; pin/unpin transitions reconcile the S3 lifecycle.
- **Secondary-index pipeline** (`index/`, `memtable/eager_index.rs`) —
  per-index state tracker, channel-based build scheduler, local/remote/off
  backends, FTI + vector (HNSW/IVFFlat) sidecars, artifact manifest.
  Registrations are dogfooded to `system_schema.indexes`; `unregister_table`
  (the DROP TABLE choke point for every DDL route) cascades tombstones over the
  dropped table's registrations via `write_index_tombstones_for_table` and
  sweeps its tracker entries (t_ae06e925). Boot-time
  `reload_indexes_from_system_schema` returns an `IndexReloadOutcome`
  (`restored`/`skipped`); unresolvable rows emit one summary warn plus the
  `ferrosa_storage_index_reload_skipped_rows_total` counter (per-row detail at
  debug). Pre-existing orphans are never GC'd automatically — clean up with
  `DROP INDEX IF EXISTS`.
- **Full-text search** (`fulltext_search`) — searches the memtable FTI + each
  per-SSTable `-FTI-{index}.db` sidecar, and **falls back to scanning any live
  SSTable whose sidecar is transiently missing** (the async index-rebuild window
  after compaction), so a stable row is never dropped from `fts_match`
  (BUG-F-007 / t_0455c0a1).
- **Snapshot / PITR** (`snapshot/`, `restore/`, `commitlog/archiver.rs`) —
  S3 snapshot manager, commit-log archiving, restore manager with validation.
- **Quarantine + self-heal** (`quarantine.rs`, `self_heal/`) — malformed rows
  found at flush/replay are written to a durable `quarantine/*.jsonl` sidecar
  instead of crashing; the self-heal controller detects corrupt SSTables and
  quarantines them under a safety rail.
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

## Public API (key entry points)

| Area | Items |
|------|-------|
| Engine | `StorageEngine`, `StorageEngineConfig`, `new`/`open`, `register_table[_with_indexes]`, `shutdown` |
| Write | `write`, `batch_write`, `write_atomic_batch`, `apply_batch`, `begin_batch`/`BatchTxn`/`BatchOp`, `replay_mutations` |
| Read | `read`, `read_range`, `read_token_range[_bounded]`, `range_iter[_projected|_fragmented]`, `count_range`, `read_by_index`, `ann_search`, `fulltext_search`, `walk_token_range[_for_digest]` |
| Maintenance | `flush`, `flush_if_needed`, `flush_all`, `poll_compactions`, `truncate`, `sync_sstables_to_s3` |
| Snapshot/PITR | `create_snapshot_with_store`, `open_from_snapshot_with_store`, `list/delete_snapshot_with_store` |
| Abstraction | `DataStore` / `LocalDataStore` (the `Arc<dyn DataStore>` boundary) |
| Config types | `CommitLogConfig`, `SyncStrategyConfig`, `CompactionConfig`, `ObjectStoreConfig`, `Mutation`, `TableId` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-cdc`** — `CdcBus` for change-data-capture emission on the write path.
- **`ferrosa-common`** — `Token`, `DecoratedKey`, `PartitionKey`, `CellValue`,
  `TableSchema`, `Result`/`Error`, cell/clustering validation.
- **`ferrosa-index`** — secondary index builders (BTree/Hash/FullText/Vector),
  FTI sidecar merge.
- **`ferrosa-schema`** — table/keyspace metadata and system schema persistence.
- **`ferrosa-sstable`** — `Partition`, `Row`, `SSTableReader`/`SSTableWriter`,
  BTI format I/O.

External: `object_store` (aws), `tokio`, `arc-swap`, `parking_lot`,
`crossbeam-skiplist`, `crc32fast`, `sha2`, `dashmap`, `serde`, `bytes`, `fs2`.

**Called by** (crates that depend on this):

- **`ferrosa`** (main binary), **`ferrosa-cluster`**, **`ferrosa-cql`**,
  **`ferrosa-ctl`**, **`ferrosa-graph`**, **`ferrosa-index-builder`**,
  **`ferrosa-loadgen`**, **`ferrosa-postgres`**, **`ferrosa-session`**,
  **`ferrosa-sparql`**.

## Tests

~1010 test functions across in-module `#[test]`/`#[tokio::test]` and 17
integration files (`tests/`), including proptest property suites
(`engine_property`, `compaction_property`, `commitlog_property`,
`property_tests`) and the repair fuzz harness (`repair_fuzz.rs`, gated behind
`test-generators`/`fuzz-fileio`). Live-infra tests are behind the
`live-infra-tests` feature + `FERROSA_TEST_*` env vars. No `#[ignore]`.

## Specs

- [Architecture overview](specs/overview.md) — module map, invariants, position
- [Data flow](specs/data-flow.md) — write path and read path (mermaid)
- [FMEA / known issues](specs/fmea.md) — failure modes ranked by RPN
- [Roadmap](specs/roadmap.md) — Now / Next / Later
