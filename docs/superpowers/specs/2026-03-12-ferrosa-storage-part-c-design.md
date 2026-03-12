# ferrosa-storage Part C: Compaction, S3 Upload, and StorageEngine

> **Status:** Approved
> **Date:** 2026-03-12
> **Depends on:** Part A (memtable, flush, merge, TableStore), Part B (commit log)

## Goal

Complete the ferrosa-storage crate by adding commit log hardening, size-tiered compaction, S3-compatible object storage upload, and a top-level `StorageEngine` that composes all subsystems into a coherent lifecycle. After Part C, ferrosa-storage is a fully functional single-node storage engine with durable S3 write-behind.

## Architecture Overview

```text
                          ┌──────────────────────────────┐
                          │       StorageEngine           │
                          │  (composes all subsystems)    │
                          └──┬────┬────┬────┬─────────────┘
                             │    │    │    │
                 ┌───────────┘    │    │    └──────────┐
                 ▼                ▼    ▼               ▼
          ┌────────────┐  ┌──────────┐ ┌───────────┐ ┌──────────────┐
          │ CommitLog   │  │TableStore│ │Compaction │ │UploadManager │
          │ (hardened)  │  │(Part A)  │ │  (STCS)   │ │(object_store)│
          └────────────┘  └──────────┘ └───────────┘ └──────────────┘
```

Write path: `CommitLog::append()` → `TableStore::write()` (serialize once, move values)

Flush pipeline: `TableStore::flush()` → `CommitLog::discard_completed()` → `UploadManager::submit()` → `CompactionStrategy::select()` → `CompactionExecutor::submit()` → manifest update

## Design Decisions

- **Compaction:** STCS only for Part C. LCS and TWCS on backlog. `CompactionStrategy` trait enables future strategies.
- **Object storage:** `object_store` crate (Apache Arrow). S3-compatible — works with MinIO, Cloudflare R2, Ceph. No AWS SDK dependency.
- **Async runtime:** Tokio handle provided by caller (`StorageEngine::new(config, runtime_handle)`). No owned runtime.
- **12-factor config:** All deployment configuration from environment variables. No hardcoded endpoints, credentials, or bucket names.
- **Zero-copy:** Minimize data copying throughout the pipeline. Mutations serialize once into the commit log buffer. SSTable files on disk are the shared artifact between flush, compaction, and upload. `Bytes` type for S3 transfers.
- **Property tests:** Liberal use of proptest throughout — statistical coverage for invariants across all subsystems.

## Section 1: Commit Log Hardening

Three targeted fixes to the existing commit log. No new public APIs.

### 1a. In-flight Write Counter

Add `in_flight_writers: AtomicU64` to `Segment`. Writers increment before `write_entry()` and decrement after. `flush_to_disk()` waits for the counter to reach zero before reading the buffer, ensuring no partially-written entries are captured.

```text
Writer:         allocate() → counter.fetch_add(1) → write_entry() → counter.fetch_sub(1)
flush_to_disk:  while counter.load() > 0 { yield } → write buffer[..pos] → fsync
```

### 1b. Incremental Flush

Replace the current "rewrite entire buffer" approach with:

- A persistent `File` handle stored in the `Segment` (opened on first flush)
- A `last_flushed_position: AtomicU64` tracking how far has been written to disk
- Each flush writes only `buffer[last_flushed..current_position]` via append

Eliminates redundant I/O on large segments.

### 1c. Sync Marker Wiring

Sync strategies call `segment.write_sync_marker()` after each flush, building the forward-linked marker chain. The reader already follows marker chains — this wires up the write side so crash recovery can skip corrupted sections by jumping to the next marker.

### Property Tests

- Concurrent writers + concurrent flushes never produce a file with a partially-written entry (validate by reading back and checking all CRCs)
- Incremental flush produces identical file content to full-buffer flush
- Sync marker chain is correctly linked (reader can follow the chain end-to-end)

## Section 2: Compaction (Size-Tiered — STCS)

### 2a. CompactionStrategy Trait

```rust
pub trait CompactionStrategy: Send + Sync {
    fn select(&self, sstables: &[SSTableMetadata]) -> Vec<CompactionTask>;
}
```

`SSTableMetadata`: lightweight struct with path/key, size, min/max token, timestamp range, level.

`CompactionTask`: identifies input SSTables to merge and output destination.

### 2b. SizeTieredStrategy

Groups SSTables into buckets by similar size (within configurable ratio, default 0.5–1.5x of bucket median). When a bucket reaches `min_threshold` (default 4), triggers compaction.

Environment variables:

| Env var | Purpose | Default |
|---------|---------|---------|
| `FERROSA_COMPACTION_MIN_THRESHOLD` | Min SSTables per bucket to trigger | `4` |
| `FERROSA_COMPACTION_MAX_THRESHOLD` | Max SSTables per compaction | `32` |
| `FERROSA_COMPACTION_BUCKET_LOW` | Lower size ratio bound | `0.5` |
| `FERROSA_COMPACTION_BUCKET_HIGH` | Upper size ratio bound | `1.5` |

### 2c. CompactionExecutor

Takes a `CompactionTask`, opens input SSTables via `SSTableReader`, merge-iterates them (reusing existing `merge_partitions` logic), writes output via `SSTableWriter`. Zero-copy where possible — partition keys and cell values as `&[u8]` passed through without cloning.

Runs on a background thread (CPU+IO bound, not async). Channel-based interface: `StorageEngine` submits tasks, receives completion notifications.

### 2d. SSTable Lifecycle

SSTables are reference-counted (`Arc`). Compaction output is written to a new file, then old SSTables are atomically removed from the active set (ArcSwap, same pattern as segment rotation). Old files deleted once last `Arc` reference drops.

### Property Tests

- Compaction of N SSTables produces output containing the union of all live data
- Tombstones suppress older data (no phantom resurrection)
- Compaction is idempotent: `compact(compact(X))` has same logical content as `compact(X)`
- Bucket selection is deterministic for the same input set

## Section 3: S3 Upload Manager

### 3a. ObjectStoreConfig

Populated entirely from environment variables:

| Env var | Purpose | Default |
|---------|---------|---------|
| `FERROSA_S3_ENDPOINT` | S3-compatible endpoint URL | (required) |
| `FERROSA_S3_BUCKET` | Bucket name | (required) |
| `FERROSA_S3_REGION` | Region | `us-east-1` |
| `FERROSA_S3_ACCESS_KEY_ID` | Credential | (from env/instance profile) |
| `FERROSA_S3_SECRET_ACCESS_KEY` | Credential | (from env/instance profile) |
| `FERROSA_S3_ALLOW_HTTP` | Allow non-TLS (MinIO local dev) | `false` |
| `FERROSA_S3_PREFIX` | Key prefix for multi-tenant | `""` |

`ObjectStoreConfig::from_env() -> Result<Self>` reads these, returns clear errors for missing required vars. Builds an `object_store::aws::AmazonS3` instance.

### 3b. UploadManager

Receives upload tasks via `tokio::mpsc` channel. Runs as a spawned tokio task on the caller-provided runtime handle.

- **SSTable upload:** Uploads component files as S3 objects under `{prefix}/sstables/{table_id}/{sstable_id}/`. Uses `object_store::put()` with `Bytes` — zero-copy from buffered SSTable files.
- **Backpressure:** Bounded channel (`FERROSA_S3_UPLOAD_QUEUE_DEPTH`, default 16). Full channel blocks flush/compaction. Prevents unbounded local disk growth.
- **Retry:** Exponential backoff on transient S3 errors (503, timeout). Fatal errors (403, 404 bucket) propagate up.

### 3c. Manifest

JSON document at `{prefix}/manifest.json` listing all live SSTables:

```json
{
  "format_version": 1,
  "sstables": {
    "ks.table": [
      { "id": "abc123", "size": 1048576, "min_token": -9223372036854775808, "max_token": 0 }
    ]
  },
  "last_compacted_at": "2026-03-12T..."
}
```

Updated atomically via conditional put (`if-match` etag for compare-and-swap). On conflict, re-read and retry.

### 3d. Local Cache Eviction

After successful S3 upload, local SSTable files become cache. Background sweep deletes local files when disk usage exceeds `FERROSA_LOCAL_CACHE_MAX_BYTES` (default 10 GB), evicting oldest-accessed first. SSTables needed for reads are re-fetched from S3 on demand via `object_store::get()`.

### Property Tests

- Upload + download round-trip preserves SSTable byte-for-byte
- Manifest CAS never loses an SSTable entry (concurrent updates both reflected)
- Backpressure: upload queue full → flush blocks → no data loss
- Cache eviction never deletes an SSTable referenced by the current manifest

## Section 4: StorageEngine Composition

### 4a. StorageEngineConfig

Aggregates all sub-configs from environment variables:

```rust
pub struct StorageEngineConfig {
    pub commit_log: CommitLogConfig,
    pub compaction: CompactionConfig,
    pub object_store: ObjectStoreConfig,
    pub local_cache_max_bytes: u64,
    pub flush_threshold_bytes: u64,
}

impl StorageEngineConfig {
    pub fn from_env() -> Result<Self> { ... }
}
```

`FERROSA_FLUSH_THRESHOLD_BYTES` (default 32 MB) controls auto-flush trigger.

### 4b. StorageEngine Public API

```rust
pub struct StorageEngine {
    tables: RwLock<HashMap<TableId, TableStore>>,
    commit_log: CommitLog,
    compaction_executor: CompactionExecutor,
    upload_manager: UploadManager,
    runtime: tokio::runtime::Handle,
}
```

- `new(config, runtime_handle)` — creates commit log, starts compaction executor, starts upload manager, replays WAL
- `write(table_id, key, row)` — commit log append → memtable put
- `read(table_id, key)` — delegates to `TableStore::read()`
- `flush(table_id)` — memtable → SSTable → discard commit log → submit upload → trigger compaction check
- `shutdown()` — flush all dirty memtables, drain upload queue, stop compaction, shutdown commit log

### 4c. Write Path (Zero-Copy)

```text
write() → CommitLog::append(mutation)    // serialize once into CAS-allocated buffer
       → TableStore::write(key, row)     // move values into memtable
```

Mutation serialized once. Memtable write takes owned data — row/cells are moved, not copied.

### 4d. Flush → Compact → Upload Pipeline

```text
flush trigger (size or timer)
  → TableStore::flush()                  // memtable → SSTable files (parallel writes)
  → CommitLog::discard_completed()       // clean WAL segments
  → UploadManager::submit(sstable)       // async S3 upload via channel
  → CompactionStrategy::select()         // check if compaction needed
  → CompactionExecutor::submit(task)     // merge SSTables on background thread
  → UploadManager::submit(compacted)     // upload compaction output
  → update manifest                      // atomic CAS in S3
```

Each arrow is a handoff — no data copying between stages. SSTable files on disk are the shared artifact.

### 4e. Recovery

`StorageEngine::new()` cold start:

1. Load manifest from S3 (list of live SSTables)
1. Fetch any SSTables not in local cache
1. `CommitLog::open_and_replay()` → replay mutations into memtables
1. Ready for reads/writes

### Property Tests

- Write N mutations → shutdown → recover → all data readable
- Concurrent writes + flushes + compactions → no data loss
- Flush threshold respected: memtable never exceeds 2x threshold
- Shutdown drains all in-flight uploads before returning

## Build Order

1. Commit log hardening (1a, 1b, 1c)
1. Compaction (2a, 2b, 2c, 2d)
1. S3 upload manager (3a, 3b, 3c, 3d)
1. StorageEngine composition (4a, 4b, 4c, 4d, 4e)

## New Dependencies

| Crate | Purpose |
|-------|---------|
| `object_store` | S3-compatible object storage (Apache Arrow) |
| `tokio` | Async runtime (handle provided by caller) |
| `bytes` | Zero-copy byte buffers for S3 transfers |

## Backlog (Not in Part C)

- Leveled Compaction Strategy (LCS)
- Time-Window Compaction Strategy (TWCS)
- io_uring I/O backend for commit log
- mmap segment buffers
