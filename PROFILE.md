# Profiling Ferrosa

This guide is for testing an optimized Ferrosa process and tuning its profiling
at runtime. The diagnostic build keeps full DWARF symbols and compiles jemalloc
with heap profiling support. Heap profiling is opt-in at process startup through
`_RJEM_MALLOC_CONF`; changing its settings does not require recompiling.
The `profiling` Cargo profile and the `ferrosa/profiling` Cargo feature are
build-time choices. They provide optimized DWARF symbols and jemalloc profiler
support; they do not force a sampling rate or begin heap dumps by themselves.

## Build

Use the curated shippable feature set plus the diagnostic allocator feature:

```bash
cargo build --locked --profile profiling \
  --target x86_64-unknown-linux-musl \
  -p ferrosa --bin ferrosa \
  --features ferrosa/full,ferrosa/profiling
```

`profiling` inherits release optimizations and retains full debug information.
`ferrosa/profiling` compiles jemalloc's heap profiler into the binary. Normal
release builds do not enable that feature and continue to use the optimized,
stripped release path. Do not use `--all-features`: it includes test-only and
unfinished features.

The installer publishes the OCI archive separately for amd64 and arm64. Choose
the architecture-specific download link from the profiling-image workflow
summary, then load it with `podman load -i oci.tar` (or the OCI tool used by your
test host).

## Test runtime heap profiling

Run a short process with `_RJEM_MALLOC_CONF` set. `prof_final:true` writes a heap
profile when the process exits:

```bash
mkdir -p /tmp/ferrosa-profiles
_RJEM_MALLOC_CONF='dirty_decay_ms:0,muzzy_decay_ms:0,prof:true,prof_active:true,prof_final:true,prof_prefix:/tmp/ferrosa-profiles/ferrosa' \
  ./target/x86_64-unknown-linux-musl/profiling/ferrosa --version
ls -l /tmp/ferrosa-profiles
```

This is also a build smoke test: a profiling binary should create a `.heap`
file. The installer image job runs this check before publishing each OCI image.

For a workload test, set the same environment on the Ferrosa process, run the
representative workload, then stop it cleanly to request the final dump. In a
container, pass the value with `podman run --env _RJEM_MALLOC_CONF='...' ...`. Mount a
writable host directory at the chosen `prof_prefix` location to retain the dump
outside the container.

The build prefixes jemalloc symbols with `_rjem_`, so its environment variable
is `_RJEM_MALLOC_CONF`. Jemalloc processes environment options after Ferrosa's
link-time defaults; include `dirty_decay_ms:0,muzzy_decay_ms:0` explicitly when
you want those values to remain obvious alongside other settings.

## Runtime tunables

The settings below can be changed for another process, request, or host run
without rebuilding the binary. Jemalloc environment options apply when a new
process starts.

| Setting | What it changes | Example or default |
|---|---|---|
| `_RJEM_MALLOC_CONF` | jemalloc options; profiling support must be compiled into this diagnostic binary | `prof:true,prof_active:true,prof_final:true,prof_prefix:/tmp/ferrosa-profiles/ferrosa` |
| `prof_active` | Turns heap sampling on or off for the process | `true` |
| `lg_prof_sample` | Heap allocation sampling interval as a base-2 exponent; smaller values collect more samples and add more overhead | jemalloc default is `19` (about 512 KiB) |
| `prof_prefix` | Prefix and directory for jemalloc `.heap` files | Use a writable mounted directory |
| `prof_final` | Writes a final heap profile when the process exits | `true` for short test runs |
| `prof_gdump` | Requests additional heap dumps as allocated memory grows | Enable for growth investigations |
| `dirty_decay_ms`, `muzzy_decay_ms` | Controls how quickly freed pages are returned to the operating system | Ferrosa's link-time defaults are both `0` |
| `FERROSA_TELEMETRY_SAMPLE_RATE` | Fraction of tracing spans sampled by Ferrosa telemetry | Default `0.01` (1%); set per process |
| `/api/debug/flamechart?seconds=N` | Duration of the authenticated tracing activity chart | Default `5` seconds, capped at `60` |
| `RUST_LOG` | Runtime tracing log filter | For example, `info` or `ferrosa=debug` |

### CQL Accord transaction bounds

These per-node CQL settings bound open Accord transaction state. They take effect
when Ferrosa starts; invalid, zero, or inconsistent values log an error and use
the defaults without stopping startup. The maximum staged-statement count includes both reads
and writes. `USING TIMEOUT` may choose a transaction deadline up to the configured
maximum.

| Environment variable | What it bounds | Default |
|---|---|---:|
| `FERROSA_CQL_TRANSACTION_MAX_OPEN` | Concurrent open CQL Accord transactions per node | `10000` |
| `FERROSA_CQL_TRANSACTION_MAX_STATEMENTS` | Staged reads plus writes in one transaction | `10000` |
| `FERROSA_CQL_TRANSACTION_DEFAULT_TIMEOUT_MS` | Open transaction lifetime when no override is supplied | `10000` ms |
| `FERROSA_CQL_TRANSACTION_MAX_TIMEOUT_MS` | Largest `BEGIN ... USING TIMEOUT` override | `600000` ms |
| `FERROSA_CQL_TRANSACTION_REAPER_INTERVAL_MS` | How often expired open transactions are evicted | `1000` ms |

These are CQL/Accord controls. They do not configure PostgreSQL transactions or
PostgreSQL MVCC history.

### Accord phase timing

The PreAccept fast-path wait is read once per process when the Accord driver is
first constructed. If the final fast-path vote does not arrive in this window,
the coordinator may switch to ballot-1 Accept only after it has actual votes
from a slow quorum; a timeout is never treated as a vote. Invalid or zero values
log an error and use the default. PostgreSQL writes also require a slow quorum
from the transaction marker key's replicas before this cutoff can advance the
protocol, so the combined multi-key replica set cannot bypass snapshot freshness.

| Environment variable | What it changes | Default |
|---|---|---:|
| `FERROSA_ACCORD_PREACCEPT_FAST_PATH_TIMEOUT_MS` | Maximum wait for a possible final fast-path PreAccept response before using an already-collected slow quorum | `1000` ms |

The default leaves time for Accept and local dependency application within the
existing 5-second read dependency wait while allowing ordinary sub-second
replica responses to retain the one-round fast path. Increase it when healthy
replica response latency regularly exceeds one second; decrease it only when
the extra Accept round is preferable to waiting for the final fast-path vote.

### SSTable write, compression, and reader buffers

These process-start settings tune SSTable output buffering, compression working
sets, and decompressed chunk-cache residency. Invalid, unreadable, or
out-of-range values log `ERROR` and fall back to defaults. Raising a limit can
increase memory use per active writer, compressor, or reader; budget it against
the number of concurrent operations.

| Setting | What it changes | Default and accepted range |
|---|---|---|
| `FERROSA_SSTABLE_WRITE_SEGMENT_BYTES` | Aligned segment size used by each component writer | `1 MiB`; positive up to `FERROSA_SSTABLE_MAX_WRITE_SEGMENT_BYTES`; rounded up to a direct-I/O block multiple with a one-time `WARN` |
| `FERROSA_SSTABLE_MAX_WRITE_SEGMENT_BYTES` | Maximum requested segment size | `16 MiB`; minimum `1 MiB`, maximum `1 GiB - 256 bytes - 4 KiB` for the per-pump safety budget and block rounding |
| `FERROSA_SSTABLE_WRITE_QUEUE_DEPTH` | Segments queued ahead of the writer | `3`; `0` selects synchronous writes; upper bound is the configured queue maximum and actual segment/depth combination must fit the 1 GiB per-pump budget |
| `FERROSA_SSTABLE_MAX_WRITE_QUEUE_DEPTH` | Maximum queue depth | `16`; minimum `3`, safety ceiling `floor(1 GiB / 257 bytes) - 1`; the active segment/depth combination has a stricter checked budget |
| `FERROSA_SSTABLE_PUMP_WAIT_WARMUP_TIMEOUT_MS` | Maximum time pump open waits to initialize the producer thread's timed blocking-wait path before writes begin | `1 ms`; `1`–`100 ms`; invalid values log an error and use the default. The free-segment channel is empty during warmup, so this normally costs up to the configured duration once per pump open |
| `FERROSA_SSTABLE_COMPRESSION_THREADS` | Compression worker count | Available parallelism capped at `4`; from `1` to the configured maximum |
| `FERROSA_SSTABLE_MAX_COMPRESSION_THREADS` | Maximum compression workers | `4`; from `4` to `64` (bounds thread stack and scheduler overhead) |
| `FERROSA_SSTABLE_COMPRESSION_BATCH_CHUNKS` | Chunks held in one compression batch | `16`; from `1` to the configured maximum; combined input and `compress_bound` output buffers per compressor must fit within `1 GiB` |
| `FERROSA_SSTABLE_MAX_COMPRESSION_BATCH_CHUNKS` | Maximum batch size | `64`; minimum `16`; also limited by the checked 1 GiB per-compressor working-set budget |
| `FERROSA_SSTABLE_COMPRESSION_CHUNK_BYTES` | Default compression chunk size | `16 KiB`; from `1 KiB` to the configured maximum |
| `FERROSA_SSTABLE_MAX_COMPRESSION_CHUNK_BYTES` | Maximum compression chunk size | `1 MiB`; minimum `16 KiB`; also limited by the checked 1 GiB per-compressor working-set budget |
| `FERROSA_SSTABLE_ROW_INDEX_MIN_ROWS` | Rows per row-index entry for wide partitions | `32`; from `1` to the configured maximum |
| `FERROSA_SSTABLE_MAX_ROW_INDEX_MIN_ROWS` | Maximum row-index spacing | `4096`; minimum `4096`; larger values are accepted and reduce index density |
| `FERROSA_SSTABLE_CHUNK_CACHE_ENTRIES` | Maximum decompressed chunks retained by one reader | `128`; any positive count, with no eager reservation |
| `FERROSA_SSTABLE_CHUNK_CACHE_BYTES` | Maximum decompressed chunk bytes retained by one reader | `8,396,800` bytes (128 × (64 KiB + 64 bytes)); from `1` byte to `1 GiB`, with no eager reservation |

The pump checks `(queue depth + 1) × (rounded segment bytes + 256 bytes)` before
allocating and falls back to defaults if the per-pump total exceeds 1 GiB.
Compression checks the input plus worst-case output buffers before allocating;
if the requested batch/chunk combination exceeds 1 GiB per compressor, it uses
the default 16 KiB chunks and 16-chunk batch. The chunk cache evicts least
recently used entries when either its byte or entry limit is reached. These are
allocation safety guards; ordinary performance tuning remains available through
the settings above.

### Storage and query throughput

These storage settings affect memory limits, write batching, read working sets,
or background concurrency. Set them in the Ferrosa container environment and
restart the process; they are read during startup or when the corresponding
worker is initialized. Byte values are bytes unless the name says otherwise.

| Setting | What it changes | Default |
|---|---|---|
| `FERROSA_FLUSH_THRESHOLD_BYTES` | Memtable size that triggers a flush | `67108864` (64 MiB) |
| `FERROSA_MEMTABLE_BACKPRESSURE_BYTES` | Active memtable limit before writes are backpressured/rejected; defaults to `max(4 × flush threshold, 64 MiB)` | `268435456` (256 MiB with the default flush threshold) |
| `FERROSA_MEMTABLE_NUM_SHARDS` | Number of memtable shards | `64` |
| `FERROSA_FLUSH_MAX_AGE_SECS` | Maximum age before a memtable is flushed | `30` |
| `FERROSA_FLUSH_PARALLELISM` | Shared flush worker count | Host available parallelism, clamped to `1..64` |
| `FERROSA_CACHE_MAX_BYTES` | Maximum local SSTable cache size | `10737418240` (10 GiB) |
| `FERROSA_CACHE_HOT_WINDOW_SECS` | Seconds after a foreground read during which a table's uploaded SSTables are never evicted from the local cache. `0` disables hotness. | `900` |
| `FERROSA_S3_REQUEST_TIMEOUT_SECS` | Per-request object-store timeout, covering the whole response body; must fit the largest SSTable component download | `900` |
| `FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES` | Free space reserved on the data filesystem; writes fail closed below it | `536870912` (512 MiB) |
| `FERROSA_CACHE_MIN_BYTES` | Minimum local cache target | `0` |
| `FERROSA_LOCAL_DISK_EVICTION_LOW_WATER_BYTES` | Free-space point that starts local SSTable eviction | `2 × FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES` |
| `FERROSA_LOCAL_DISK_EVICTION_TARGET_FREE_BYTES` | Free-space target after eviction | `max(low water, 3 × reserve)` |
| `FERROSA_SSTABLE_READER_CACHE_CAP` | Maximum idle SSTable readers retained in the shared LRU pool | `256` |
| `FERROSA_READ_MERGE_FANIN` | SSTable readers opened at once for a staged token-range merge | `32` |
| `FERROSA_RANGE_READ_ROWS_PER_FRAGMENT` | Rows retained/emitted per range-read fragment | `4096` |
| `FERROSA_RANGE_SPILL_THRESHOLD_BYTES` | Absolute memory threshold before a range result spills to local disk; overrides the percentage threshold | No absolute override |
| `FERROSA_RANGE_SPILL_THRESHOLD_PCT` | Memory percentage threshold before range-result spill | Runtime memory budget default |
| `FERROSA_COMMITLOG_BATCH_TARGET_BYTES` | Pending WAL bytes that trigger a group sync | `65536` (64 KiB) |
| `FERROSA_COMMITLOG_BATCH_MAX_DELAY_MICROS` | Maximum time a dirty WAL batch waits before sync | `10000` (10 ms) |
| `FERROSA_COMPACTION_WORKERS` | Compaction worker threads | Host CPU count, bounded by implementation limits |
| `FERROSA_MAX_CONCURRENT_COMPACTIONS` | Number of compactions allowed to execute concurrently | Auto-tuned from CPU and memory limits |
| `FERROSA_COMPACTION_READAHEAD_BYTES` | Read-ahead window for direct compaction scans | SSTable scan default |
| `FERROSA_INDEX_SIDECAR_TIMEOUT_MS` | Timeout for a remote index-sidecar request | `30000` (30 s) |
| `FERROSA_BACKGROUND_MAX_BLOCKING` | Blocking-task thread cap for the background runtime | Runtime default |
| `FERROSA_DATA_RUNTIME_THREADS` | Data runtime worker threads | `8` |
| `FERROSA_CQL_RUNTIME_THREADS` | CQL runtime worker threads | `8` |
| `FERROSA_BACKGROUND_RUNTIME_THREADS` | Background runtime worker threads | `2` |

Direct I/O has separate switches for SSTable writes and compaction input scans.
`FERROSA_SSTABLE_DIRECT_IO` controls immutable `Data.db` writes. Compaction input
scans use `FERROSA_COMPACTION_DIRECT_READ`; `FERROSA_DIRECT_IO` is its fallback
when the compaction-specific switch is unset. Both paths are on by default;
set the relevant value to `0` to use buffered I/O. An unsupported filesystem
falls back to buffered I/O for SSTable writes and records a fallback metric.
These switches do not change write durability. Benchmark each mode on the same
device and workload because bypassing the page cache can reduce cache pollution
while changing repeated-read performance.

For write throughput, compare commit-log batch size/delay and flush parallelism
one change at a time. For memory pressure, watch memtable backpressure, reader
pool pressure, local cache size, and range spill together. Raising a buffer or
worker limit can move pressure to the page cache, local disk, or cgroup rather
than remove it. Keep the profiling workload and resource limits fixed between
runs.

### PostgreSQL MVCC and SQL

PostgreSQL MVCC and SQL resource bounds can be tuned at process startup without
rebuilding Ferrosa:

| Environment variable | What it bounds | Default |
|---|---|---:|
| `FERROSA_POSTGRES_MAX_TXN_WRITES` | Mutations buffered by one PostgreSQL transaction before it fails with a resource-limit error | `10000` |
| `FERROSA_POSTGRES_SCAN_BUFFER_ROWS` | Rows buffered between a storage scan producer and the synchronous SQL executor | `64` |
| `FERROSA_POSTGRES_MVCC_MAX_SNAPSHOT_AGE_MS` | Maximum lifetime of an active PostgreSQL snapshot/transaction; later use fails with SQLSTATE `40001` | `600000` ms |
| `FERROSA_POSTGRES_MVCC_SNAPSHOT_REAPER_INTERVAL_MS` | Background cadence for expiring old snapshots and pruning history they retain | `1000` ms |

Every value must be a positive integer. If any PostgreSQL override is invalid,
Ferrosa logs an error and uses the complete default set without stopping
startup. The scan buffer bounds rows in flight from storage; it does not limit
the number of rows returned by a query. Query results are streamed to the client
in small fixed batches with socket backpressure, so response memory does not grow
with result size; increasing this buffer only changes the storage-side producer
window. A portal suspended by `Execute` with `max_rows` keeps one blocking
executor thread until it resumes or is closed.

The maximum snapshot age bounds how long an abandoned or long-running
transaction can retain old row versions. Once expired, its next query or commit
fails with `40001`; the reaper removes its active lease and allows history
pruning. Choose an age that accommodates legitimate transaction duration and
tune the sweep interval separately if reclamation latency matters.
In cluster mode, PostgreSQL commits submit the snapshot and read/write table set
through Accord, and replica apply carries row-version metadata to support active
snapshots on other nodes. The opt-in native-driver Jepsen workload records
transfers, register updates, exact-key reads of two doctor rows plus an absent
phantom key, and write-skew transactions. It does not check general SQL range
predicates. Its single-replica pause schedule checks the history
and final state on the active quorum. It does not verify catch-up of the resumed
replica or serializability across concurrent CQL and PostgreSQL operations.

Use a lower `lg_prof_sample` value to capture more allocation events. Start with
the default when measuring workload latency, since heavier sampling can change
the result. jemalloc accepts `_RJEM_MALLOC_CONF` at startup; changes apply to a new
process and do not need a new binary.

The debug flamechart endpoint samples Ferrosa activity and renders active query
and connection labels. It is not an instruction-level CPU profile. Use Linux
`perf` for CPU samples; the host kernel and permissions control whether the test
host can collect them:

```bash
sudo perf record -F 99 --call-graph dwarf -p "$FERROSA_PID" -- sleep 60
sudo perf report
```

`-F 99` changes the CPU sampling frequency without recompiling. The profiling
binary's DWARF information lets `perf` resolve native stack frames.

## Before sharing a result

Record the OCI architecture, Ferrosa source revision shown in the workflow
summary, exact runtime environment values, workload and duration. Compare against
the same workload on the normal release image; the profiling build is meant for
diagnostics, not as a production image.
