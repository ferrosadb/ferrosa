---
crate: ferrosa-storage
doc: data-flow
last_updated: 2026-06-30
---

# ferrosa-storage — Data Flow

Two canonical paths: the **write path** (memtable → commit log → flush → S3) and
the **read path** (memtable → cache → S3). All view transitions are
`ArcSwap<StoreView>` swaps, so reads are wait-free and only flushes contend (on a
per-table `Mutex`).

## Write path

A write is made durable by the commit log first, then applied to the memtable.
A memtable that crosses the flush / backpressure thresholds is sealed and flushed
to a BTI SSTable, which is then uploaded to S3 as write-behind.

```mermaid
flowchart TD
    FE["Front-end / cluster<br/>Arc&lt;dyn DataStore&gt;"] --> W["StorageEngine::write / write_atomic_batch"]
    W --> ADM{"admission:<br/>local_disk_free_reserve_bytes OK?"}
    ADM -- no --> FAIL["fail closed<br/>(before commit-log append)"]
    ADM -- yes --> CL["CommitLog::append<br/>CAS alloc into active segment"]
    CL --> SYNC{"SyncStrategyConfig"}
    SYNC -- Batch --> FSYNC["fsync every write<br/>(zero loss window)"]
    SYNC -- "Periodic (default)" --> TIMER["background fsync on timer<br/>(up to sync_interval loss window)"]
    SYNC -- Group --> GRP["batched fsync<br/>(bounded by max_wait)"]
    FSYNC --> PUT
    TIMER --> PUT
    GRP --> PUT
    PUT["ArcSwap::load StoreView<br/>active.put → one memtable shard<br/>(cell-level merge-on-write)"] --> CDC["WriteObserver / CdcBus emit"]
    PUT --> BP{"memtable_size &gt;= backpressure_bytes?"}
    BP -- yes --> FL
    BP -- no --> DONE["ack write"]

    subgraph FlushPath["Flush (per-table Mutex; reads/writes continue)"]
      FL["flush_guard.lock<br/>swap in fresh memtable<br/>old → flushing"] --> SER["snapshot + sort<br/>build_serialization_header"]
      SER --> WR["FlushTarget::flush → BTI SSTable<br/>(+ FTI / vector sidecars)"]
      WR --> VERIFY{"write_verify?"}
      VERIFY -- yes --> RB["reopen + self-readback"]
      VERIFY -- no --> PUB
      RB --> PUB["ArcSwap: prepend SstableDescriptor<br/>clear flushing"]
      PUB --> COMP["maybe_compact (STCS / UCS)"]
      PUB --> UP["UploadManager: submit components"]
    end

    UP --> S3["S3 (object_store)<br/>SHA-256 integrity meta<br/>authoritative durable store"]
    UP --> PLOG["pending-upload log<br/>(replayed after crash)"]
    UP --> MAN["Manifest: etag CAS update"]
    PUB -. "PinMode::NvMe → skip S3 upload" .-> CACHEPIN["LocalCache pinned set"]
```

## Read path

A read loads an immutable `StoreView`, checks the in-memory tiers, then opens
only the SSTables whose key/token bounds could contain the key — filling pages
from the local cache and falling back to S3. Results are merged newest-first with
cell-level last-write-wins.

```mermaid
flowchart TD
    R["StorageEngine::read / read_range / read_token_range"] --> LOAD["ArcSwap::load StoreView<br/>(wait-free)"]
    LOAD --> MEM["check active memtable<br/>→ Option&lt;Arc&lt;Partition&gt;&gt;"]
    LOAD --> FLM["check flushing memtable<br/>(if mid-flush)"]
    LOAD --> PRUNE["prune SSTable descriptors<br/>by key / token bounds"]
    PRUNE --> POOL["ReaderPool::get_or_open<br/>engine-wide LRU, cap 256"]
    POOL --> CACHE{"LocalCache hit?"}
    CACHE -- yes --> RD["SSTableReader::get_partition<br/>(bloom filter internal)"]
    CACHE -- "miss / cold page" --> FETCH["fetch from S3<br/>verify SHA-256"]
    FETCH --> REG["register in LocalCache<br/>(pinned entries never evicted)"]
    REG --> RD
    MEM --> MERGE
    FLM --> MERGE
    RD --> MERGE["merge_partitions<br/>cell-level LWW, newest-first<br/>tombstone suppression"]
    MERGE --> OUT["Option&lt;Partition&gt; / Vec&lt;Partition&gt; → front-end"]
    FETCH -. "integrity mismatch" .-> ERR["IntegrityError → re-fetch once,<br/>then fatal for that component"]
```

## Notes

- **Durability boundary.** A write is durable only after its commit-log entry is
  fsynced. Under the default `Periodic` strategy that fsync is deferred up to
  `sync_interval`, so an acked write can be lost on an unclean stop (FMEA ST-1).
  `Batch` closes the window at a latency cost.
- **S3 is authoritative.** Local NVMe is a write-behind cache. Cache eviction
  removes only local copies of remotely-durable objects; manifest-pinned entries
  are never evicted, and `PinMode::NvMe` tables deliberately keep no S3 copy.
- **Crash recovery.** On `open`, the engine first loads the size-bounded,
  discriminated registry `schema.json`; if no registry snapshot exists, a
  storage-only consumer can use the separately owned `storage-schema.json`.
  Both files stream through bounded serde readers, and the storage list is
  published through stage/fsync/verify/atomic-rename/directory-fsync rather
  than a whole-file buffer. The engine then replays commit-log segments into
  memtables (bounded) and the pending-upload log so in-flight S3 uploads
  complete. Pending-upload replay scans both flat
  SSTable components and generation-directory components produced by S3 restore.
- **Quarantine.** A row that fails cell/clustering validation at flush or replay
  is written to a durable `quarantine/*.jsonl` sidecar and skipped; the flush
  continues and `FLUSH_QUARANTINED_ROWS_TOTAL` increments.
