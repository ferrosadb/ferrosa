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

### Internode reconnect (ferrosa-net)

A lane whose peer connection drops retries quickly, then slowly and
indefinitely, until the peer returns or the peer is removed. A peer whose pool
could not be replaced is re-dialed by the heartbeat loop on the same schedule.
These are read when a retry cycle or probe is scheduled, so a changed value
applies to the next one. A value that is not a positive integer is ignored with
one warning per setting and the default is used.

| Environment variable | What it changes | Default |
|---|---|---:|
| `FERROSA_NET_RECONNECT_FAST_ATTEMPTS` | Connect attempts per fast-phase cycle (exponential backoff 1 s doubling to 30 s). After three exhausted cycles the lane enters slow-retry | `10` |
| `FERROSA_CONNECT_TIMEOUT_MS` | Bound on DNS resolution and on the TCP connect, each, for every outbound dial (fast reconnect, slow-retry probe, peer re-dial). The handshake keeps its own `FERROSA_HANDSHAKE_TIMEOUT_SECS`. One lane dial is therefore bounded by 2 x this + the handshake timeout (15 s at defaults), below the 30 s slow interval; the next probe is scheduled only after the previous dial ends, so cadence is interval + dial time, never stuck behind a hung connect | `5000` ms |
| `FERROSA_NET_RECONNECT_SLOW_INTERVAL_MS` | Interval between single-attempt probes in slow-retry, and the cap on a pool-less peer's re-dial backoff. Up to 25% random jitter is added so nodes do not dial in lockstep | `30000` ms |

Logging is edge-only: one line when a lane drops into slow-retry and one when
it reconnects, however long the outage. Per-attempt detail is DEBUG, and every
attempt is counted by `ferrosa_net::reconnect::total_reconnect_attempts`.

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
barrier abstain bound (`FERROSA_ACCORD_BARRIER_TIMEOUT_SECS`, 5 s by default)
while allowing ordinary sub-second replica responses to retain the one-round fast
path. Increase it when healthy replica response latency regularly exceeds one
second; decrease it only when the extra Accept round is preferable to waiting for
the final fast-path vote.

### Accord dependency-wait bounds

Two bounds govern how long an Accord transaction waits on the replicas that
ordered before it. They are separate settings on purpose, because the two waits
have opposite costs, and both are per-process values read once and cached in a
lock-free atomic — changing them needs a restart, not a rebuild.

- **Apply bound.** How long the coordinator waits for its ordered dependencies to
  reach `Applied` before it abandons the transaction. Abandoning rolls the
  transaction back (it is never applied), releases any successor parked behind
  it, and tells the client the transaction was not committed and may be retried
  (PostgreSQL SQLSTATE `40001`; CQL a retryable server error). Raise this for a
  slow mutator: a client updating many rows in one transaction legitimately needs
  a longer window than a benchmark burst. It is single-sourced from the epoch
  drain period's `DEFAULT_TXN_TIMEOUT` (10 s) because the drain is sized as
  `SkewMax + DEFAULT_TXN_TIMEOUT` — raising this without raising the drain would
  let a drain cut off a transaction still inside its bound.
- **Barrier abstain bound.** How long the PostgreSQL snapshot-barrier read-vote,
  and every inbound `ReadVote`, waits for its conflicting transactions to reach
  `Applied` before it abstains. Deliberately tighter than the apply bound:
  raising it only makes a *failing* transaction slower. An abstain is fail-loud
  and the client retries, so there is nothing to wait longer for. Keep it at or
  below the apply bound.

| Environment variable | What it bounds | Default |
|---|---|---:|
| `FERROSA_ACCORD_TXN_TIMEOUT_SECS` | Apply bound: how long a transaction may wait for its dependencies before it is abandoned (rolled back, client told to retry) | `10` s |
| `FERROSA_ACCORD_BARRIER_TIMEOUT_SECS` | Barrier bound: how long the snapshot-barrier read-vote and inbound `ReadVote` wait before abstaining | `5` s |

A non-numeric, zero, or negative value logs one warning and uses the default.
Zero is refused rather than clamped: it would fail every transaction the instant
it parked.

### SSTable write, compression, and reader buffers

These process-start settings tune SSTable output buffering, compression working
sets, and decompressed chunk-cache residency. Invalid, unreadable, or
out-of-range values log `ERROR` and fall back to defaults. Raising a limit can
increase memory use per active writer, compressor, or reader; budget it against
the number of concurrent operations.

The **codec** itself is a separate, coarser trade: it buys on-disk size (and so
bytes moved to object storage) with CPU on every flush and compaction output.
A host with more CPU than storage bandwidth should compress harder; a
bandwidth-rich, CPU-poor host should not.

| Setting | What it changes | Default and accepted range |
|---|---|---|
| `FERROSA_SSTABLE_COMPRESSION` | Default codec for tables whose schema selects none. Set-but-empty is treated as unset, so `fly machine update --env KEY=` clears it | `lz4`; one of `lz4`, `zstd`, `none` |
| `FERROSA_SSTABLE_ZSTD_LEVEL` | Zstd level for that codec and for the zstd schema fallback | `3`; `-7`–`22` |

A per-table `compression.class` schema extension always wins over these; the
knobs only supply the fallback. The extension is matched on its **last dotted
segment**, so `org.apache.cassandra.io.compress.ZstdCompressor` and
`ZstdCompressor` are equivalent:

| `compression.class` | Codec |
|---|---|
| `LZ4Compressor`, `LZ4`, `lz4` | LZ4 |
| `ZstdCompressor`, `Zstd`, `zstd`, `ZSTD` | Zstd (level from `compression.compression_level`/`compression.level`, else `FERROSA_SSTABLE_ZSTD_LEVEL`) |
| `NoCompressor`, `NoopCompressor` | none |
| empty, `none`, `null`, `false` | none |
| `compression.enabled` = `false`/`0` | none (checked first, wins over `compression.class`) |

`NoCompressor` is the class a Cassandra `ALTER TABLE ... WITH compression =
{...}` writes to disable compression, so a table a real Cassandra client created
uncompressed stays uncompressed here instead of failing with
`UnsupportedCompression`. Any other unrecognized name is still rejected loudly.

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
The three cache and object-store timeout settings also have a TOML key (shown
in the table); the file wins over the environment variable, which wins over the
default, and a TOML value that is not a whole number aborts startup.

| Setting | What it changes | Default |
|---|---|---|
| `FERROSA_FLUSH_THRESHOLD_BYTES` | Memtable size that triggers a flush | `67108864` (64 MiB) |
| `FERROSA_MEMTABLE_BACKPRESSURE_BYTES` | Active memtable limit before writes are backpressured/rejected; defaults to `max(4 × flush threshold, 64 MiB)` | `268435456` (256 MiB with the default flush threshold) |
| `FERROSA_MEMTABLE_NUM_SHARDS` | Number of memtable shards | `64` |
| `FERROSA_FLUSH_MAX_AGE_SECS` | Maximum age before a memtable is flushed | `30` |
| `FERROSA_MAX_DEFERRED_REPLAY_MUTATIONS` | Startup commit-log replay: most mutations held in memory for tables that are absent while a schema exists. Overflow is written to `<data_dir>/commitlog-unreplayed/` and logged at ERROR with table ids and counts. Must be a positive integer; an invalid value fails startup. | `10000` |
| `FERROSA_FLUSH_PARALLELISM` | Shared flush worker count | Host available parallelism, clamped to `1..64` |
| `FERROSA_CACHE_MAX_BYTES` (TOML `[storage] cache_max_bytes`) | Maximum local SSTable cache size. Local disk is a cache over the object store: size it well below the data, never to hold it all. CI and dev clusters run with 1 GiB so eviction and read-back are always under test, and the cache invariant suite (`ferrosa-storage/src/engine/cache_invariants.rs`) exercises that path on every PR. | `10737418240` (10 GiB) |
| `FERROSA_CACHE_HOT_WINDOW_SECS` (TOML `[storage] cache_hot_window_secs`) | Seconds after a foreground read during which a table's uploaded SSTables are never evicted from the local cache. `0` disables hotness. | `900` |
| `FERROSA_S3_REQUEST_TIMEOUT_SECS` (TOML `[s3] request_timeout_secs`) | Per-request object-store timeout, covering the whole response body; must fit the largest SSTable component download | `900` |
| `FERROSA_S3_MAX_CONCURRENT_REQUESTS` | The in-flight target: the cap on concurrent object-store requests (one `LimitStore` shared by uploads, deletes, restore and reads) and the floor for the connection pool. It is a property of the link, not of this machine: in-flight data to fill a wire is bandwidth x round-trip time, so `requests = bandwidth x RTT / request size`. Never derived from cores or worker counts. Positive integer | `64` (WAN: assumes 1 Gbit/s to R2/S3 at 100 ms RTT = 12.5 MB in flight / ~256 KiB per request ~ 50, rounded up). Counted in requests, not bytes, because `object_store` exposes no byte limiter; convert with the formula. Read observed RTT and throughput from `ferrosa_s3_request_duration_seconds` and `system_observability.object_store_stats` (`FERROSA_S3_STATS=1`) to tune it. **Raising this alone does nothing for downloads:** they put at most `FERROSA_S3_DOWNLOAD_PART_CONCURRENCY` x `FERROSA_RESTORE_CONCURRENCY` (default 4 x 4 = 16) requests in flight, so the target is unreachable until those two are raised too |
| `FERROSA_S3_POOL_MAX_IDLE_PER_HOST` | Idle connections kept per host. Overrides the derived value, which is the in-flight target above. Rejected at startup if below the in-flight target (a smaller pool reconnects on every burst). Effective value is logged at store construction ("object store client built") and exported as `ferrosa_s3_pool_max_idle_per_host` and `ferrosa_s3_max_in_flight` when `FERROSA_S3_STATS=1` | the in-flight target (`64`) |
| `FERROSA_S3_POOL_IDLE_TIMEOUT_SECS` | How long an idle pooled connection is kept | `90` |
| `FERROSA_S3_CONNECT_TIMEOUT_SECS` | Object-store TCP+TLS dial timeout. This is NOT `FERROSA_CONNECT_TIMEOUT_MS`, which governs internode connections; the two are unrelated | `10` |
| `FERROSA_S3_DOWNLOAD_PART_BYTES` | Size of one ranged GET when downloading a component; objects at or below it use a single GET. Positive integer; invalid values stop startup | `16777216` (16 MiB) |
| `FERROSA_S3_DOWNLOAD_PART_CONCURRENCY` | Ranged parts in flight per object. All parts share the one store, so `FERROSA_S3_MAX_*` caps still apply. A 429 shrinks that object's concurrency by one slot | `4` |
| `FERROSA_RESTORE_CONCURRENCY` | SSTable generations restored at once at startup | `4` |
| `FERROSA_S3_STATS` | `1`/`true` collects object-store stats (per-operation counts, bytes, latency, 429s; per-table/component bytes, object sizes, throughput, read amplification) and exposes them as `ferrosa_s3_*` Prometheus series and `system_observability.object_store_stats` / `object_store_ops`. Per-table labels exist only when on | off |
| `FERROSA_S3_STATS_MAX_KEYS` | Distinct `(table, component)` label pairs the stats layer tracks. This bounds metric label CARDINALITY, not query results: further pairs fold into `component=overflow` (logged once) so no observation is dropped, and a read of `system_observability.object_store_stats` still returns every tracked row. Raise it if a tenant has more than ~4,000 table/component pairs and the overflow bucket is in use. Read once at first use; a non-positive or malformed value warns and uses the default | `4096` |
| `FERROSA_S3_READ_PAGE_BYTES` | Page size for ranged reads of an evicted SSTable's `Data.db` (compressed tables round up to whole chunks). Minimum 4096; an invalid value logs a warning and uses the default. Larger pages mean fewer requests and more bytes per point read. | `1048576` |
| `FERROSA_S3_PAGE_CACHE_BYTES` | Memory the evicted-SSTable page cache may hold. `0` disables caching (every read refetches). | `268435456` |
| `FERROSA_RESTORE_EVICTED_MODE` | Startup handling of evicted SSTables: `remote` registers them with index components only and reads `Data.db` by ranged GETs; `full` downloads every component first. | `remote` |
| `FERROSA_RESTORE_HOT_TABLES_ON_START` | `0` disables the background pass that fully restores recently read tables' evicted SSTables (it stops before free disk drops under the eviction target). | `1` |
| `FERROSA_LOCAL_DISK_FREE_RESERVE_BYTES` | Free space reserved on the data filesystem; writes fail closed below it | `536870912` (512 MiB) |
| `FERROSA_CACHE_MIN_BYTES` | Minimum local cache target | `0` |
| `FERROSA_EVICTION_AUDIT_MAX_BYTES` | Hard cap on the on-disk eviction audit (`<data_dir>/eviction-audit/`), split into 4 ring segments after a 4 KiB reserve; clamped to 16 KiB..32 MiB. Under 0.8% of the default free-space reserve | `4194304` (4 MiB) |
| `FERROSA_EVICTION_AUDIT_OFFLOAD` | `true` uploads rotated audit segments to `<prefix>/eviction-audit/<instance>/` through the throttled object store: one segment per sync, one attempt, local copy removed only after the upload succeeds | off |
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
| `FERROSA_CQL_MAX_CONNECTIONS` | Maximum concurrent CQL client connections accepted (`[cql] max_connections`) | `1024` |
| `FERROSA_CQL_MAX_CONNECTIONS_PER_IP` | Maximum concurrent CQL connections from one peer address (`[cql] max_connections_per_ip`) | `64` |
| `FERROSA_CQL_MAX_IN_FLIGHT_PER_CONNECTION` | Concurrent CQL requests admitted per client connection before the node replies `Overloaded` (`"request backpressure"`). Also settable as `[cql] max_in_flight_per_connection`. **Raise only alongside a latency win:** sustained throughput is bounded by `in-flight ÷ service time`, so exceeding this valve converts a clean shed into unbounded queueing and tail latency. Values must be positive integers; an unparsable or zero value stops startup rather than silently defaulting | `128` |

The three `FERROSA_CQL_*` limits above were previously only reachable by editing the
source — `max_in_flight_per_connection` in particular was hardcoded and appeared nowhere
in the config surface, so an operator could not raise the valve that causes
`Overloaded("request backpressure")` under high concurrency. All three now accept TOML
(which wins) or env. Precedence matches every other tunable here: `[cql] <key>` overrides
`FERROSA_<KEY>`, which overrides the default.

The eviction audit answers "why did the cache evict?" after the log has rotated.
Each eviction pass that finds pressure appends one JSON line: trigger, cache cap,
floor, free-space target, projected free space, the manifest's byte claim and the
real on-disk total of the same generations (the gap is the signal), how many
generations were evicted and their size, and the writer's pid and build.
Identical consecutive passes coalesce into one record with a count. The latest
pass is on `/metrics` as `ferrosa_storage_eviction_audit_*` gauges. Retention is
the ring itself: the newest segments that fit the cap, oldest dropped first;
offload is the only way to keep more.

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

### jemalloc page decay

The binary ships a compile-time `malloc_conf` of `dirty_decay_ms:0,
muzzy_decay_ms:0`, which returns freed pages to the OS as soon as they are
released — the right default under a memory cap, but a trade against allocation
throughput. `malloc_conf` is consumed by jemalloc **before `main` runs**, so it
cannot be overridden from TOML or from ferrosa's own environment plumbing.
These two knobs are applied through jemalloc's runtime control API (`mallctl`)
as the first thing `main` does, before any tokio runtime is built.

| Setting | What it changes | Default |
|---|---|---|
| `FERROSA_JEMALLOC_DIRTY_DECAY_MS` | `arenas.dirty_decay_ms`: how long a freed dirty page is retained for reuse before being purged | unset — leaves jemalloc's `malloc_conf` value (`0`, purge immediately) |
| `FERROSA_JEMALLOC_MUZZY_DECAY_MS` | `arenas.muzzy_decay_ms`: the same for muzzy (already returned to the OS, still mapped) pages | unset — leaves `malloc_conf` (`0`) |

A negative value means **never purge** (favours throughput and retains RSS); `0`
purges immediately. Unset or empty leaves jemalloc untouched, so these are no-ops
until set. Each write is read back and the effective value is logged; the root
`opt.*` mallctl names look like the obvious targets but are read-only and return
`EPERM`, which is the silent-no-op failure mode these knobs guard against.
`background_thread` is deliberately **not** exposed: this jemalloc build lacks
background-thread runtime support, so the mallctl node does not exist at all.

### PostgreSQL MVCC and SQL

PostgreSQL MVCC and SQL resource bounds can be tuned at process startup without
rebuilding Ferrosa:

| Environment variable | What it bounds | Default |
|---|---|---:|
| `FERROSA_POSTGRES_MAX_TXN_WRITES` | Mutations buffered by one PostgreSQL transaction before it fails with a resource-limit error | `10000` |
| `FERROSA_PG_COMMIT_PROFILE` | Per-phase attribution for a PostgreSQL commit (prepare, Accord order/gate, apply fan-out, MVCC observer, prune, WAL) on the coordinator. Unset = zero cost | unset |
| `FERROSA_ACCORD_COMPRESSION` | Codec for the Accord apply **region** body: `none` (default), `lz4`, `snappy`, `zstd`. Opt-in: compression costs CPU and only pays when the transport term is byte-bound rather than deserialize-bound | `none` |
| `FERROSA_ACCORD_COMPRESSION_LEVEL` | Codec level where the codec has one (zstd); ignored by the fixed-level codecs | `3` |
| `FERROSA_ACCORD_COMPRESSION_BLOCK_BYTES` | Block size the region is compressed in — the knob that moves the ratio/CPU/memory trade | `262144` (256 KiB) |
| `FERROSA_ACCORD_COMPRESSION_MIN_BYTES` | Frames below this size are sent uncompressed (headers + CPU lose to just sending them) | `65536` (64 KiB) |
| `FERROSA_ACCORD_CONFLICT_INDEX_CAPACITY` | Keys one Accord transaction may register in the conflict index. Defaults to `FERROSA_POSTGRES_MAX_TXN_WRITES` (floored at `100000`), so the front end cannot admit a transaction consensus is unable to register | derived |
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

#### Accord apply-region compression: measured

`FERROSA_ACCORD_COMPRESSION` is opt-in and default-`none`; this is the first
measurement of it. Every row is a 3-node **loopback** cluster receiving ONE
transactional `COPY` of **exactly N = 100,000 rows** and its COMMIT. N is verified
per row from the logs: the follower applies total 100,000 mutations
(31,798 + 35,180 + 33,022 across the three nodes). `serialize_ms` is the
coordinator's frame build (region copy + capnp header + codec), so the codec's own
cost is its delta against `none`. `frame_bytes` is the largest per-peer wire frame.

| setting | frame_bytes | ratio | serialize_ms | codec ms | fanout_ms | max_ack_ms | RSS n1/n2/n3 (MB) |
|---|---:|---:|---:|---:|---:|---:|---:|
| `none` (default) | 21,671,344 | 1.00x | 82.1 | — | 1086 | 1043 | 608 / 376 / 368 |
| `lz4` | 3,774,361 | 5.74x | 576.8 | +494.7 | 1582 | 1285 | 568 / 380 / 371 |
| `lz4` blk 64 KiB | 3,805,439 | 5.69x | 583.4 | +501.2 | 1615 | 1318 | 573 / 386 / 363 |
| `lz4` blk 1 MiB | 3,767,994 | 5.75x | 554.2 | +472.1 | 1563 | 1279 | 520 / 378 / 371 |
| `snappy` | 4,234,224 | 5.12x | 217.7 | +135.5 | 1208 | 1097 | 527 / 387 / 370 |
| `zstd` | 2,055,220 | 10.54x | 198.7 | +116.6 | 1154 | 1053 | 536 / 373 / 367 |
| `zstd` blk 64 KiB | 2,008,826 | 10.79x | 196.7 | +114.6 | 1158 | 1059 | 554 / 383 / 368 |
| `zstd` blk 1 MiB | 2,117,670 | 10.23x | 212.8 | +130.6 | 1167 | 1059 | 537 / 377 / 367 |

What the numbers say:

* **`zstd` wins on both axes** — the lowest build cost (+115–131 ms) *and* the best
  ratio (10.2–10.8x). `lz4` is the worst here on both: 5.7x for the most expensive
  build of all (+472–501 ms). `snappy` matches `lz4`'s ratio (5.1x) for a sixth of
  its CPU.
* **Block size barely moves the result.** `zstd` is 10.2–10.8x from 64 KiB to 1 MiB
  and `lz4` is 5.7x at every block; the 256 KiB default is fine. The smallest block
  was marginally best for `zstd` (10.79x) with no measured CPU penalty.
* **RSS is flat across codecs** (peaks within ~40 MB of `none`): compression does not
  materially change coordinator or peer memory — the compressed frame is transient.
* **On this loopback cluster, compression LOSES on latency.** `fanout_ms` rises from
  1086 ms (`none`) to 1154–1615 ms, because every node is on one host: the transport
  term is ~free, so the only effect of a codec is added CPU on the critical path.
  The byte saving can only pay for itself when the transport is byte-bound, i.e.
  across a real network — which this measurement does NOT exercise.

> **Two honesty caveats.**
> (1) The ratio is a **benchmark artifact, not a general claim**: the pgbench/pgcopy
> `accounts` filler column is one repeated character, near-ideal input for any LZ
> codec. Expect far less than 5–11x on real payloads.
> (2) This is a **3-node loopback** run. It characterises the codec's CPU / ratio /
> memory cost, NOT its benefit on the wire. Do not read it as "enable compression
> for throughput".

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
