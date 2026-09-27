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

The PostgreSQL MVCC path currently has no runtime environment tunables. Snapshot
versions are reclaimed automatically after the oldest active snapshot advances;
the PostgreSQL write-set cap is a fixed code limit of 10,000 mutations. Do not
set undocumented MVCC environment variables or treat that cap as configurable.
In cluster mode, PostgreSQL commits submit the snapshot and read/write table set
through Accord, and replica apply carries row-version metadata to support active
snapshots on other nodes. Native-driver cross-node coverage is present; run the
Jepsen PostgreSQL strict-serializability workload before treating that model as
system-wide evidence.

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
