---
crate: ferrosa-storage
status: implemented
last_updated: 2026-06-30
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

Its upstream boundary is the `DataStore` trait: front-ends hold
`Arc<dyn DataStore>` rather than `Arc<StorageEngine>`, so the same call sites
serve standalone (`LocalDataStore`) and cluster-routed deployments. Its downstream
boundary is `ferrosa-sstable` (BTI I/O) and `object_store` (S3). It knows nothing
about CQL/SQL protocol framing or query planning — those belong to the front-ends.

## Module map

| Module | Responsibility |
|--------|----------------|
| `engine` (`src/engine.rs`, ~18.8k LoC) | `StorageEngine` + `StorageEngineConfig`: composition root; write/read/range/batch API, registration, snapshot/PITR orchestration, maintenance |
| `store` (`src/store.rs`, ~9.2k LoC) | `TableStore`: lock-free `ArcSwap<StoreView>` per table; flush serialization; reader-pool wiring; index/FTI sidecar flush |
| `memtable/` | `Memtable` trait; `SkipListMemtable` (default), `ShardedBTreeMemtable`; eager-index + vector-index hooks |
| `commitlog/` | Segmented WAL: `segment` (CAS alloc), `sync` (Batch/Periodic/Group), `reader` (replay), `archiver` (S3/PITR), `cdc`, `checkpoint`, `manifest` |
| `flush` | `FlushTarget` trait + `FileFlushTarget`/`InMemoryFlushTarget`; serialization-header construction |
| `merge`, `range_merger` | Read-path cell-level LWW merge; streaming range/token-range merge |
| `compaction/` | `CompactionExecutor`, STCS + UCS strategies, `CompactionGate`, validator (oracle + differential) |
| `upload/` | `UploadManager` (tokio task), `ObjectStoreConfig`, pending-upload log + replay across flat and generation-dir SSTable layouts |
| `cache`, `pin_config` | `LocalCache` LRU + pinning; NVMe `PinMode` |
| `index/` | Index state tracker, build scheduler, local/remote/off backends, artifact manifest, virtual table |
| `snapshot/`, `restore/` | S3 snapshot manager + restore manager + validation (PITR) |
| `quarantine`, `self_heal/` | Malformed-row quarantine sidecar; deterministic self-heal control loop + corrupt-SSTable detector |
| `accord/` | Per-shard conflict index + protocol log for Accord transactions |
| `timeseries/` | Ring aggregation, late-data, WASM aggregates, materialization |
| `data_store` | `DataStore` trait + `LocalDataStore` |
| `metrics`, `virtual_tables`, `observer`, `subscription_observer` | Prometheus metrics; system virtual tables; write observers (CDC/SUBSCRIBE) |
| `batchlog` | Batchlog manager for atomic multi-partition batches |

## Data flow

**Write path** (front-end → durable): build a `Mutation` → `commit_log.append`
(CAS allocation into the active segment, durability governed by the sync
strategy) → `ArcSwap::load` the `StoreView` → `active.put` into one memtable
shard (cell-level merge-on-write). When the active memtable crosses
`memtable_backpressure_bytes`, `write()` performs a synchronous in-line flush
before returning. On flush: a per-table `Mutex` serializes; a fresh memtable is
swapped in and the old one becomes `flushing` (writes resume immediately); the
flushing snapshot is serialized to a BTI SSTable via `FlushTarget`; the new
descriptor is prepended; index/FTI sidecars are built; the SSTable components are
submitted to `UploadManager` for S3 write-behind; STCS/UCS is evaluated.

**Read path** (durable → front-end): `ArcSwap::load` (wait-free) a `StoreView` →
check active memtable → check flushing memtable → prune SSTable descriptors by
key/token bounds → open only candidate readers through the engine-wide LRU
reader pool (filling cold pages from `LocalCache`, falling back to S3) →
`merge_partitions` cell-level LWW newest-first. See [data-flow.md](data-flow.md)
for the mermaid diagrams.

## Key invariants

1. **S3 is authoritative; local disk is a write-behind cache.** Cache eviction
   must never delete the only copy — manifest-pinned entries are never evicted.
   **Exception — local `file://` backend** (`FERROSA_LOCAL_STORE_PATH` /
   `[s3].local_path`): the local disk *is* the authoritative durable store, so
   `ObjectStoreConfig::is_local()` is threaded into `LocalCache` as `durable` and
   eviction is disabled entirely (`evict_if_needed` is a no-op). The local
   backend has no conditional-PUT (CAS) support, so manifest saves use the
   unconditional path; this is safe because a single node is the sole writer.
2. **Reads are wait-free; flush never blocks reads/writes.** All view
   transitions go through `ArcSwap`; only flushes contend, on a per-table `Mutex`.
3. **Cell-level last-write-wins everywhere.** Memtable merge-on-write, read-path
   merge, and compaction all resolve conflicts by `(column_index, timestamp)`;
   tombstones (partition/row/cell) suppress older data by `marked_for_delete_at`.
4. **Durability is governed by the sync strategy.** Only `Batch` fsyncs every
   write; the **default `Periodic`** has a bounded loss window (`sync_interval`).
5. **Malformed data is quarantined, not dropped or crashed on.** A row that
   fails cell/clustering validation at flush/replay is written to a durable
   `quarantine/*.jsonl` and the counter `FLUSH_QUARANTINED_ROWS_TOTAL`
   increments — non-zero in steady state is an alert.
6. **Compaction correctness is gated by a validator.** Oracle + differential
   checks confirm a compaction output is row-equivalent to its inputs.

## Position in the dependency graph

A heavyweight internal hub. Depends on `ferrosa-cdc`, `ferrosa-common`,
`ferrosa-index`, `ferrosa-schema`, `ferrosa-sstable`. Depended on by `ferrosa`,
`ferrosa-cluster`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-graph`,
`ferrosa-index-builder`, `ferrosa-loadgen`, `ferrosa-postgres`,
`ferrosa-session`, `ferrosa-sparql`. See the root crate index for the full graph.
