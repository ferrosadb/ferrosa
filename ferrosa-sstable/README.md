# ferrosa-sstable

> The Cassandra-compatible **BTI SSTable** reader/writer — the crate's on-disk
> data layer. Reads and writes the 8-component BTI (Big Trie-Indexed) format
> over backing-store-agnostic positional I/O traits.

## What this crate is

`ferrosa-sstable` owns the binary, on-disk SSTable format. It reads and writes
the **BTI (Big Trie-Indexed)** format that is the default in Apache Cassandra
5.x — trie-indexed partition/row indexes, delta-encoded rows against a
serialization header, LZ4/Zstd compression, and a Cassandra-compatible bloom
filter. All I/O is synchronous and routes through the `ReadAt`/`WriteAt` traits,
so the same reader/writer logic runs over a local file (`FileReadAt`) or an S3
object (`S3ReadAt`, which lives in `ferrosa-storage`) without a runtime
dependency in this crate.

The crate is deliberately format-only: it knows about partitions, rows, cells,
liveness, and deletion markers, but nothing about CQL planning, schema
resolution beyond the serialization header, or cluster routing.

## What's implemented

- **BTI write** — `SSTableWriter` accepts partitions in token order and emits
  all components (Data.db, Partitions.db, Rows.db, Filter.db,
  CompressionInfo.db / CRC.db, Statistics.db, Digest.crc32, TOC.txt) either as
  in-memory buffers (`SSTableOutput`) or staged files (`SSTableOutputFiles`). A
  self-readback verification pass (`WriteOptions::verify_output`, default on)
  reopens the finished table and checks the partition count.
- **BTI read** — `SSTableReader` opens a table from component handles and serves
  point lookups (`get_partition`, `get_partition_limited_rows`,
  `get_clustering_row`), bloom/bounds pre-checks (`may_contain_key`), and
  streaming iteration in token order (`partitions_iter` → `PartitionIter`) with
  token-seek (`seek_to_token`) and projection variants.
- **Component output (T-040)** — every physical SSTable component write uses
  `AlignedPump`, including metadata and buffered Data.db output from the memory
  writer. Metadata uses depth 0: one aligned segment, exact logical-length
  truncation and sync before completion. Direct mode follows the same runtime
  switch as Data.db; `DirectWriter` honors the configured segment size while
  remaining synchronous. The streaming CompressionInfo header retains its existing
  buffered depth-0 path. `finish()` still returns owned component bytes for
  memory callers. T-081 streams completed partition/row trie nodes and CRC
  words directly to component pumps, reuses the row trie across partitions,
  and emits Bloom words from its existing bitset without a serialized copy.
- **Deferred component sync** — `finish_to_directory` returns individually
  durable files. The internal deferred-sync finish path is only for file-backed
  outputs immediately handed to a `FlushTarget::flush_deferred_files` target;
  that transaction owns staged-file sync and publication durability. Do not
  publish or retain deferred outputs as independently durable SSTables.
- **Source checksums (T-011)** — `Digest.crc32` (a single CRC32 over the final
  on-disk Data.db bytes) is written for every table; `CRC.db` (a per-chunk
  CRC32 table, Cassandra-compatible layout) is written for uncompressed
  tables. Both are computed while Data.db is written, never by a separate
  re-read. A caller opts a reader into verification with
  `SSTableReader::load_digest` / `load_crc_table`; once loaded,
  `verify_digest()` checks the whole file and every uncompressed chunk read
  is checked against `CRC.db`, naming the failing chunk's byte offset on
  mismatch. An SSTable without these components (older than T-011, or opened
  without loading them) reads exactly as before — "not checked", logged once
  per generation, never an error. See `checksum` module docs for the exact
  on-disk formats.
- **Complex (non-frozen collection) columns** — `list`/`set`/`map` columns
  read and write Cassandra's per-element cell layout: `uvint(cell-count)` then
  one cell per element, each with a length-prefixed cell path (list → TimeUUID,
  set → element, map → key). Read, write, and the projection/skip paths handle
  it; the element value uses the collection's element/value type. `marshal`
  detects multicell-ness from the type string (`ListType(..)` vs
  `FrozenType(..)`). The writer validates each partition's cell layout before
  writing it (`validate_row_cells`) and refuses a violation, such as a live
  path-less cell on a complex column, with `Error::InvalidData` naming the
  column, partition key and row, instead of panicking the calling thread
  (FMEA ST-17).
- **Trie index** — on-disk trie walker + builder (`trie/`) backing the partition
  index (Partitions.db) and the row index (Rows.db) for wide clustered
  partitions.
- **Compression** — `Compression::{None, Lz4, Zstd { level }}` with per-chunk
  CRC32 validation on read. `compress_bound(len)` / `compress_into(src, dst)`
  (T-036) compress into a caller-owned buffer instead of returning a fresh
  `Vec`, for the streaming writer's `ChunkCompressor` to reuse per chunk.
  Into-buffer API per codec: `None` is a plain
  `copy_from_slice`; `Lz4` uses `lz4_flex::block::{compress_into,
  get_maximum_output_size}` with a manually-written 4-byte length prefix
  (matching `compress_prepend_size`'s on-disk format); `Zstd` drives
  `zstd_safe::CCtx` directly through `compress_stream`/`end_stream` (the
  same calls `zstd::encode_all` makes under its `Write`-based `Encoder`),
  reusing one `CCtx` per thread — `zstd::bulk::Compressor::compress_to_buffer`
  (`ZSTD_compress2`) was tried first but produces a different frame header
  (it auto-pledges the input length; `encode_all` never does), and the
  streaming `write::Encoder` wrapper matches byte-for-byte but allocates a
  fresh 32 KiB `Vec` per call with no way to reuse it. `compress_into`'s
  output is proven byte-identical to `compress`'s for every codec
  (`compress_into_matches_compress_*`, `ferrosa-sstable/src/compression.rs`).
  **`Lz4` is not fully allocation-free**: `lz4_flex` 0.11's public block API
  always allocates a fresh match-finding hash table per call (no reusable
  state is exposed); `tests/compress_into_alloc.rs` measures and documents
  this rather than hiding it — `None` and `Zstd` are zero-allocation after
  one warm-up call, `Lz4` is a bounded one allocation per call.
- **Bloom filter** — Cassandra-compatible double-hashing over the Murmur3
  `h1`/`h2` pair from `ferrosa-common`.
- **Corruption resilience** — `validate_data_extent` (index-vs-data truncation
  check), `salvage` (best-effort per-partition recovery with `SalvageStats`),
  and a hard `MAX_VALUE_LEN` ceiling that rejects bogus on-disk lengths before
  they drive a pathological allocation.
- **Tooling binaries** — `ferrosa-sstable-dump` and `ferrosa-sstable-import`.

## Runtime tuning

Writer and reader settings are read when the component opens. Compression pool
settings are read once per process. Invalid or unreadable values log `ERROR`
and use the documented default. Maximum settings default to the historical
ceilings and can be raised after budgeting the added memory.

### Write pump

| Setting | Default | Behavior |
|---|---:|---|
| `FERROSA_SSTABLE_WRITE_SEGMENT_BYTES` | 1 MiB | 1 byte to configured maximum; rounds up to a direct-I/O block multiple with a one-time `WARN`. |
| `FERROSA_SSTABLE_MAX_WRITE_SEGMENT_BYTES` | 16 MiB | Minimum accepted maximum is 1 MiB; safety ceiling is `1 GiB - 256 bytes - 4 KiB` for block rounding and metadata. |
| `FERROSA_SSTABLE_WRITE_QUEUE_DEPTH` | 3 | 0 to configured maximum; 0 selects synchronous writes. |
| `FERROSA_SSTABLE_MAX_WRITE_QUEUE_DEPTH` | 16 | Minimum accepted maximum is 3; hard safety ceiling is `floor(1 GiB / 257 bytes) - 1`. |

An asynchronous pump holds up to `queue_depth + 1` aligned segments, or about
`(queue_depth + 1) * rounded_segment_bytes` per active component writer. It
also creates reusable segment-descriptor and iovec vectors once when the pump
opens. These two metadata allocations replace the former fixed stack iovec;
the allocation regression test checks that they do not grow per batch, segment,
or write. Larger batches are split at the OS `pwritev` vector limit. The
`DirectWriter` honors the segment setting but always uses queue depth zero.
The pump rejects an actual segment/queue combination above 1 GiB, counting
rounded segment bytes plus a conservative 256-byte descriptor allowance per
segment, with an ERROR and both defaults. This is a per-pump allocation safety
guard; lower queue depths can use larger segments within the same budget.

### Compression and row index

| Setting | Default | Accepted range |
|---|---:|---|
| `FERROSA_SSTABLE_COMPRESSION_THREADS` | Available parallelism capped at 4 | 1 to configured maximum |
| `FERROSA_SSTABLE_MAX_COMPRESSION_THREADS` | 4 | 4 to 64 threads |
| `FERROSA_SSTABLE_COMPRESSION_BATCH_CHUNKS` | 16 | 1 to configured maximum |
| `FERROSA_SSTABLE_MAX_COMPRESSION_BATCH_CHUNKS` | 64 | 16 to `usize::MAX`, subject to the 1 GiB working-set guard |
| `FERROSA_SSTABLE_COMPRESSION_CHUNK_BYTES` | 16 KiB | 1 KiB to configured maximum |
| `FERROSA_SSTABLE_MAX_COMPRESSION_CHUNK_BYTES` | 1 MiB | 16 KiB to `usize::MAX`, subject to the 1 GiB working-set guard |
| `FERROSA_SSTABLE_ROW_INDEX_MIN_ROWS` | 32 | 1 to configured maximum |
| `FERROSA_SSTABLE_MAX_ROW_INDEX_MIN_ROWS` | 4096 | 4096 to `usize::MAX` |

Increasing compression chunk and batch sizes increases working memory. Each
batch uses input buffers of about `batch_chunks * chunk_bytes`, plus output
buffers up to `batch_chunks * compress_bound(chunk_bytes)`. Their checked sum
may not exceed 1 GiB; an oversized combination logs ERROR and uses default
chunk and batch sizes before allocation. Thread count adds concurrent work
and codec state; its MAX setting is capped at 64 to bound native thread stack
and scheduler overhead. `WriteOptions::chunk_size` defaults to 16 KiB;
the chunk-bytes environment setting supplies that default when using
`WriteOptions::default()`. Explicit values are preserved when they fit the
working-set budget.

### Reader chunk cache

| Setting | Default | Accepted range |
|---|---:|---|
| `FERROSA_SSTABLE_CHUNK_CACHE_ENTRIES` | 128 | Any positive integer; no eager reservation |
| `FERROSA_SSTABLE_CHUNK_CACHE_BYTES` | 8,396,800 bytes (about 8 MiB) | 1 byte to 1 GiB; no eager reservation |

The byte default is `128 * (64 KiB + 64 bytes)`, preserving approximately 128
entries for common 64 KiB chunks and charging an estimated 64 bytes for each
key, handle, and LRU/hash entry. The per-reader budget cannot exceed 1 GiB;
larger or invalid values log ERROR and use the 8 MiB default. The non-preallocating LRU evicts least
recently used entries until both limits are met. A chunk larger than the entire
byte budget is returned but not cached. Both limits apply to decompressed and
CRC-verified uncompressed chunks. Allocator overhead and `Arc`s retained by
callers are outside resident cache accounting.

These maximum settings externalize performance ceilings, not format limits.
On-disk validation limits and the DIO alignment ceiling remain correctness or
platform safety checks. Queue storage and aligned allocation representability
are the remaining pump maxima; the kernel vector limit is handled by splitting
batches across syscalls.

## What is NOT implemented (honest scope)

- **Big-format (legacy `*-big-*`) reading** — out of scope; the crate targets
  BTI only. There is no Big-format read path (deferred per ADR-004).
- **Range tombstone markers** — not encoded by the writer; the reader skips
  them. Documented in `data.rs` / `writer.rs` as deferred.
- **Non-frozen collections and UDTs** are supported (see above). A non-frozen
  UDT is a complex column whose per-field cell path is a 2-byte big-endian field
  position (`marshal::is_nonfrozen_udt`); field values assemble via
  `ferrosa_row_bridge::collection::assemble_udt`. A complex `DeletionTime` (from a
  collection/UDT overwrite) is now captured as a `path=None` tombstone sentinel,
  round-trips writer↔reader, and is applied at assembly. Complex framing is gated
  on `SerializationHeader.complex_collections`, default `false` = Ferrosa's legacy
  whole-value storage; Cassandra import sets it `true`. Living on the header, it
  flows to every `DataReader`/`SSTableWriter` uniformly (a per-SSTable format
  switch), and is **persisted** in Statistics.db (a trailing byte after the
  `max_timestamp` Ferrosa extension; a Cassandra header lacks it → `false`), so a
  complex SSTable round-trips across reopen (`verify_output` included). Still
  deferred: **tuple** complex columns; the engine setting it `true` on flush
  (D-write emits per-element cells); and deriving `true` when importing a
  Cassandra *BTI* SSTable with collections via `SSTableReader` (its header has no
  Ferrosa byte — today collection/UDT imports read `Data.db` directly through
  `DataReader` with an explicit header). Tracked in t_83c4f093 / t_b7cec413.
- **Snappy / Deflate compression** — only None / LZ4 / Zstd are supported.

## How it works

| Module | Responsibility |
|--------|----------------|
| `checksum` | `DigestCrc32` / `ChunkCrc` (writer-side streaming checksums), `ChunkCrcTable` (reader-side CRC.db parse + verify state) — `Digest.crc32` and `CRC.db` formats |
| `io` | `ReadAt`/`WriteAt` positional traits, `FileReadAt`/`FileWriteAt`, bounded block cache (`CachedReadAt`) |
| `direct` | `DirectWriter` — page-cache-bypassing sequential writer (O_DIRECT/`F_NOCACHE`) for immutable Data.db output. On by default; `FERROSA_SSTABLE_DIRECT_IO=0` (or the master `FERROSA_DIRECT_IO=0`, which the specific switch overrides) selects the buffered writer, read at run time. A file system that rejects O_DIRECT falls back to buffered, WARN-logged and counted; byte-identical to the buffered path (see `data_db_writer_direct_matches_buffered_bytes_and_offsets`). **T-032: `DirectWriter` is now a thin wrapper over `pump::AlignedPump` at `depth = 0`.** Its public API (`create`/`mode`/`position`/`write_all`/`finish`) and on-disk behavior are unchanged — same tests pass unchanged — but the block it aligns to now comes from the real T-031 `dio_align` probe instead of a hardcoded `MIN_BLOCK` (4096), so Linux hosts whose true device alignment exceeds 4096 now write to it correctly instead of silently over-aligning |
| `direct` (read side) | `DirectReadFile` — cache-bypassing positional reader (O_DIRECT / `F_NOCACHE`, aligned bounce buffer, any offset/length). Fallback to buffered reads + `POSIX_FADV_DONTNEED` is WARN-logged and counted in `direct_read_fallbacks_total` |
| `dio_align` | `resolve_block`/`probe`/`block_for` — probes the true O_DIRECT alignment for a file via a raw `SYS_statx` syscall with `STATX_DIOALIGN` (D4), floor `MIN_BLOCK` (4096), ceiling `MAX_BLOCK` (64 KiB). Works on **gnu and musl** (the shipped `make build-musl` binary included): rather than call `libc::statx` — which `libc` 0.2.186 only compiles for `target_env = "gnu"` (the `stx_dio_*` fields, the FFI decl, and `STATX_DIOALIGN` are gated on a build-script-detected `musl_v1_2_3` cfg the crate doesn't control) — `probe` defines its own `#[repr(C)]` `KernelStatx` mirroring the stable kernel UAPI struct (compile-time size/offset asserts) and calls it through `libc::syscall(libc::SYS_statx, …)`, using only `libc::syscall`/`SYS_statx`/`AT_EMPTY_PATH`, which compile unconditionally on every Linux target. An `Unsupported` probe (old kernel, `ENOSYS`, or a filesystem reporting nothing usable) counts in `dio_align_probe_fallbacks_total` (rendered by `direct::render_prometheus`) and falls back to `MIN_BLOCK`; a probed value above `MAX_BLOCK` is `block_for`'s `Err(TooLarge)`, meaning the caller must not open that file with O_DIRECT at all. Logged once per device (`st_dev`) at INFO, bounded to 64 devices. **T-032 consumes this**: `pump::FileSink::create` calls `block_for` and, on `TooLarge`, falls back to buffered I/O (loud + counted in `direct_write_fallbacks_total`, the same counter `direct::open_bypassing`'s own O_DIRECT-rejection fallback uses) |
| `pump` | `PumpConfig` is read when each writer opens. Segment size defaults to 1 MiB and queue depth to 3 (0 is synchronous). Their default maximums are 16 MiB and depth 16; operators can raise them with `FERROSA_SSTABLE_MAX_WRITE_SEGMENT_BYTES` and `FERROSA_SSTABLE_MAX_WRITE_QUEUE_DEPTH`. Invalid settings log ERROR once per process and use defaults; segment size rounds up to a block multiple with a WARN. An async pump holds approximately `(depth + 1) * rounded segment bytes` of aligned buffers per component writer and allocates reusable iovec metadata once per pump. Larger batches split at the OS `pwritev` vector limit. `DirectWriter` uses the configured segment size at depth zero. `SegmentSink` is the seam every SSTable component write uses; `FileSink` is the production implementation. For depth ≥1, `AlignedPump::open_with_depth` starts a dedicated OS flusher thread and coalesces queued segments into `pwritev` calls. On backpressure, a nonblocking select runs first; a genuine wait uses `select!`'s deadline-based `default(duration)` arm for a one-time 10s stall watchdog. Once it fires, the pump blocks on data or abort without periodic wakeups. A stall logs WARN and increments `write_pump_stalls_total`, then logs once on recovery. Flusher errors and panics reach the producer; `finish` and `Drop` join the flusher. Thread-spawn failure falls back to synchronous mode and increments `write_pump_sync_fallbacks_total`. `write_pump_inflight_segments` counts segments sent to a flusher and not yet written back, summed over open pumps (at most `depth + 1` per pump); each sent segment carries an RAII token that releases it exactly once, and a release below zero holds the gauge at 0 and logs ERROR once instead of wrapping (FMEA ST-19). The pump still uses its local `AbortSignal`/`NeverAbort` shim; it does not yet consume `ferrosa_common::CancelToken` on this branch (T-021). `AlignedPump`, `SegmentSink` and `FileSink` are wired into file-backed writer, flush and compaction output (T-038–T-045). |
| `scan` | `ReadAheadReader<R>` — one bounded window plus a background prefetch of the next window (≤ 2 windows resident, ≤ 1 read in flight). `FileReadAt::open_scan` composes it over `DirectReadFile` for compaction input; `parse_scan_window` validates `FERROSA_COMPACTION_READAHEAD_BYTES` |
| `reader` | `SSTableReader`, `PartitionIter`, point lookup, salvage, token-summary seek index |
| `writer` | `SSTableWriter`, `WriteOptions`, `SSTableOutput[Files]` |
| `data` | Data.db row/cell codec (delta-encoded against the header) |
| `trie` | On-disk trie node, walker, builder |
| `partition_index` / `row_index` | Trie-backed Partitions.db / Rows.db |
| `compression` | `Compression` enum, chunk compress/decompress |
| `bloom` | Cassandra-compatible bloom filter |
| `statistics` | Statistics.db + `SerializationHeader` |
| `byte_comparable` | Byte-comparable key encoding for the index |
| `varint` / `marshal` | VInt codec + Cassandra `AbstractType` marshalling |
| `toc` | TOC.txt read/write + standard component lists |
| `types` | `Partition`, `Row`, `CellValue` shapes, `LivenessInfo`, `DeletionTime` |

**Write path**: caller adds `Partition`s in token order → cells delta-encoded
against the `SerializationHeader` → Data.db, with bloom + partition trie +
(for wide partitions) row trie built alongside → `finish()` emits all components
and (by default) self-verifies.

**Read path**: `get_partition` checks the bloom filter, walks the partition trie
to a Data.db offset, then decodes the partition (decompressing chunks through a
bounded LRU when compressed). Streaming reads walk Data.db directly with
constant per-partition memory.

## Legacy nanosecond timestamps (t_cf637b6e)

SSTables written before t_cf637b6e can hold Accord timestamps in
nanoseconds. `DataReader` normalises every decoded timestamp (cell, liveness,
row, complex and partition deletion) with `ferrosa_common::normalize_cell_ts`,
so readers only ever see microseconds. Two headers are kept:

- `SSTableReader::stored_header()` is Statistics.db as written: its
  `min_timestamp` is the delta base, and a `DataReader` must be built from it.
- `SSTableReader::header()` has normalised bounds (exact for ns-only files,
  widened for mixed or unknown-max files) and is what compaction, purge and
  metadata use.

`may_hold_legacy_ns_timestamps()` reports a file whose stored bounds show
legacy values; `count_legacy_ns_timestamps()` decodes the file once and
counts them. Each `DataReader` adds what it normalised to the
`sstable` source of the counter when dropped.

## Public API (key entry points)

| Area | Items |
|------|-------|
| I/O traits | `ReadAt`, `WriteAt`, `FileReadAt`, `FileWriteAt` |
| Direct I/O | `direct::DirectWriter` (page-cache-bypassing sequential writer: O_DIRECT/`F_NOCACHE`), `direct::DirectReadFile`, `DirectMode`, `direct_write_{fallbacks,files,bytes}_total`, `direct_read_{fallbacks,files,bytes}_total` |
| DIO alignment probe | `dio_align::{resolve_block, probe, block_for, ProbeResult, TooLarge, MAX_BLOCK}`, `dio_align::dio_align_probe_fallbacks_total` |
| Write pump | `pump::PumpConfig` (`from_env`, `effective_segment`), `pump::{SEGMENT_BYTES_ENV, QUEUE_DEPTH_ENV}`; `pump::{AlignedPump, SegmentSink, FileSink, AbortSignal, NeverAbort}` (T-033, `pub`); metrics `pump::write_pump_{blocked_seconds_total_free, inflight_segments, stalls_total, aborts_total, sync_fallbacks_total}` |
| Scan / read-ahead | `scan::ReadAheadReader::{new, with_prefetch}`, `FileReadAt::{open_scan, is_scan}`, `scan::parse_scan_window` |
| Reader | `SSTableReader::{open, get_partition, get_clustering_row, may_contain_key, partitions_iter, seek_to_token, salvage, validate_data_extent, load_crc_table, load_digest, verify_digest}`, `SSTableComponents` |
| Writer | `SSTableWriter::{new, new_file_backed, add_partition, finish, finish_to_directory}`, `WriteOptions`, `SSTableOutput`, `SSTableOutputFiles` |
| Checksums | `checksum::{DigestCrc32, ChunkCrc, ChunkCrcTable, digest_bytes, format_digest, parse_digest, compute_chunk_crc}` |
| Types | `Partition`, `Row`, `LivenessInfo`, `DeletionTime`, `Compression` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `Token`, `DecoratedKey`, `PartitionKey`, `CellValue`,
  Murmur3 hashing, `Error`/`Result` (the shared types the format encodes).

External: `crc32fast`, `crossbeam-channel` (the `depth >= 1` write pump's
`full`/`free`/error channels and stall-watchdog `select!`, T-033 — D7), `libc`
(O_DIRECT/`F_NOCACHE`/`posix_fadvise`/`pwritev` for `direct::DirectWriter` and
`pump::FileSink`), `lru`, `lz4_flex`, `memmap2`, `rayon`, `tracing` (loud
fallback logging), `zstd`. **No async runtime** — positional I/O is
synchronous; S3 wrappers live in `ferrosa-storage`.

**Called by** (crates that depend on this):

- `ferrosa-cdc`, `ferrosa-cluster`, `ferrosa-cql`, `ferrosa-ctl`, `ferrosa-graph`,
  `ferrosa-index-builder`, `ferrosa-loadgen`, `ferrosa-postgres`,
  `ferrosa-row-bridge`, `ferrosa-schema`, `ferrosa-sparql`, `ferrosa-storage`,
  `ferrosa-worker`.

## Tests

In-crate unit tests across every module (trie, data codec, varint, bloom,
byte-comparable, statistics, reader/writer round-trips) plus integration suites:
`tests/cassandra_compat.rs` (binary-exact oracle vs Cassandra fixtures),
`tests/property_tests.rs` (proptest round-trips), and
`tests/p0_production_disk_replay.rs` (real on-disk replay regression).

### Write pump tests (`pump_sync_`, `pump_async_`, `pump_loom_`, `pump_stress_`)

`pump_sync_*` (depth 0, T-032) and `pump_async_*` (depth ≥ 1, T-033/T-034)
live in `src/pump.rs` under `#[cfg(test)]`, plus `tests/pump_sync_alloc.rs`,
`tests/pump_async_alloc.rs`, `tests/pump_loom.rs` and `tests/pump_stress.rs`
(all require `--features test-support`). The `pump_async_` suite covers: the
full 11-`Fault` `FaultySink` matrix (5 hard failures — `Eio`, `Enospc`,
`Panic`, `FsyncFail`, `SetLenFail` — and the 6 silent corruptions) at depth
1–4 (T-034 completed the matrix; T-033 covered a subset); mode/depth parity
(0..=4 byte-identical to the same `RecordingSink`); bounded-in-flight-then-
blocks and one-permit-at-a-time resume (BP1/BP2/BP4 — 25 randomized gate
schedules by default, 10 000 under `--release`, all byte-identical; see
below); failure and abort while parked, both returning promptly with no
leaked thread (BP5/BP6/CD1); the stall watchdog firing exactly once past
`STALL_THRESHOLD` (shrunk to 50 ms under `cfg(test)`) with one WARN and one
recovery INFO (CD2/BP8); `pwritev` coalescing ≥ 3 contiguous segments into one
call, deterministically (5 segments split any way between at most two
flusher sweeps always leaves the larger sweep ≥ 3 — CD3); and an L6 park-count
contention budget via a `thread_local!` counter (CD4).

**Allocation (`pump_async_alloc.rs`).** Separates a fully deterministic
zero-allocation proof (writing exactly `depth` more never-recycled segments
after a one-segment channel warm-up) from two measurements under a real,
concurrently-running producer/flusher handoff: an unthrottled sink (small,
scheduler-jitter-bounded residue) and a permit-gated sink forcing a genuine
park on almost every write. T-034 replaced the stall watchdog's
`crossbeam_channel::after()` (ST-16: allocated a fresh one-shot timer channel
on every genuine park) with `select!`'s own `default(duration)` arm, which
removes that specific allocation — but **does not** make a genuine park
allocation-free outright: isolated measurement against bare
`crossbeam_channel` found that `select!` itself, and a plain non-`select!`
`Receiver::recv()` alike, cost roughly one allocation of their own on every
genuine park (most likely a fresh wait-queue node, since a parked waiter
needs a stable heap address the wake side can find and the previous park's
node cannot be reused). This is a property of `crossbeam_channel` itself,
independent of `after()`, and out of scope here (decisions.md D2 already
names the fix — a hand-rolled SPSC ring — and defers it, "gated on data, not
done up front"). `pump_async_alloc.rs`'s module doc has the exact numbers;
its tests assert a tight, park-count-proportional bound, not a literal zero,
under sustained backpressure.

**Concurrency model (`tests/pump_loom.rs`, `--features loom`-only).** A
`loom` model of the producer/flusher protocol (decisions.md D2/D7): one
producer, one flusher, `depth` ∈ {1, 2}, with a flusher error and a flusher
panic injected at every segment index across every interleaving
`loom::model` explores. Since loom cannot instrument `crossbeam_channel`'s
own internals, the model runs against a small `loom::sync`-based channel
shim (`send`/`recv`/`try_recv`/`select2`), not the real crossbeam-backed
pump — see the file's module doc for why that is still a faithful test of
the *protocol's* liveness and exactly-once-accounting properties. `loom` is
an *optional* dependency gated by this crate's own `loom` Cargo feature, not
the `RUSTFLAGS="--cfg loom"` + unconditional-dev-dependency convention loom's
own docs usually recommend: this crate depends on `ferrosa-common`, which
depends on `tokio`, and `tokio` has its own internal `#[cfg(loom)]`-gated
code that only compiles correctly under tokio's OWN loom test harness setup
— a global `RUSTFLAGS` cfg reaches `tokio` too and breaks its build
(confirmed directly: `unresolved import crate::sync::AtomicWaker` in
`tokio::task::local`). The Cargo feature scopes cleanly to this crate alone.
Run with `cargo test -p ferrosa-sstable --release --features
test-support,loom pump_loom_`.

**Stress (`tests/pump_stress.rs`).** 64 concurrently open pumps, each with an
independently randomized segment size, queue depth, byte count and (for
about half of them) one injected `Fault`, inside a 60 s wall budget enforced
by `Receiver::recv_timeout` against a shrinking deadline (never an unbounded
`join()`). Asserts every pump reports an outcome, `pump::live_flusher_threads()`
(new T-034 test-only instrumentation — a `Drop` guard around each flusher
thread's body) returns to `0` once every pump is dropped, and a process-wide
peak-tracking allocator's high-water mark stays within a generous multiple of
`sum over pumps of (depth + 1) * segment`. Uses `StressSink`, a fault-capable
sink that folds bytes into a running CRC32 instead of retaining them like
`RecordingSink` does, so the test double's own memory doesn't dominate the
measurement. Run with `cargo test -p ferrosa-sstable --release --features
test-support pump_stress_`.

### Writer test oracle (T-035) and the golden SSTable corpus

`tests/oracle.rs` freezes today's `SSTableWriter` output as a byte-exact
oracle, ahead of later write-pump packets (T-030-series,
`ferrosa-suite/specs/sstable-write-pump/`) rewriting how `writer.rs` produces
`Data.db`. It drives the writer's existing public API (`SSTableWriter::new` +
`finish`, and `new_file_backed` + `finish_to_directory`) rather than a copied
implementation — see the module doc in
`tests/support/legacy_writer.rs` for why a verbatim internals copy wasn't
worth it at this commit.

- `tests/support/generators.rs` — proptest generators for synthetic
  `(schema, WriteOptions, partitions)` inputs: 0-5 clustering columns (fixed-
  and variable-length CQL types), static rows, row/partition deletions,
  TTL/expiring cells, simple and complex (non-frozen collection) columns,
  empty values, and row bodies from 0 B up to ~256 KiB. Reuses
  `ferrosa_common::test_generators::arb_decorated_key` (the `test-generators`
  feature) for partition keys rather than hand-rolling one.
- `tests/support/legacy_writer.rs` — `ComponentBytes`, `legacy_write`/
  `legacy_write_file_backed` (the two writer entry points, captured into one
  shape), and `assert_components_identical` (names the first differing
  component and byte offset).
- `tests/support/golden.rs` — the `CASES` table (~20 small SSTables:
  uncompressed and every supported codec, chunk sizes 4K/16K/64K, file-backed
  and in-memory, plus deliberate large-row and chunk-straddling cases) and
  the read/write helpers for `tests/golden/<case name>/`.
- `tests/golden/` — the checked-in corpus: one directory per case
  (`Data.db`, `Partitions.db`, `Rows.db`, `Filter.db`, `Statistics.db`,
  `CompressionInfo.db` when compressed, `TOC.txt`) plus `manifest.txt`
  (seed, options, and a SHA-256 per component).
- `tests/golden_regen.rs` — regenerates the corpus. **Never runs
  destructively in normal `cargo test`/CI**: only
  `FERROSA_REGEN_GOLDEN=1 cargo test -p ferrosa-sstable --test golden_regen
  -- --nocapture` rewrites `tests/golden/`. Without the env var it still
  checks the manifest matches `CASES`, so it's a real assertion either way,
  never a silent no-op.

`oracle_*` tests (`tests/oracle.rs`): `oracle_golden_reproduction` (today's
writer reproduces every golden file byte-for-byte),
`oracle_file_backed_matches_in_memory` (the two writer entry points agree,
1000 proptest cases), and `oracle_golden_reads_back_through_reader` (golden
files read back through `SSTableReader` with the expected partition/row
counts).

### `compress_into` (T-036)

`compress_into_*` tests in `src/compression.rs` (`compress_into_matches_compress_{none,lz4,zstd}`
plus their `_prop_*` proptest siblings over input lengths `0..=65536`, and
`compress_into_dst_too_small_errs`) prove `compress_into`'s output is
byte-identical to `compress`'s and that an undersized `dst` errors without
writing anything. `tests/compress_into_alloc.rs` is a separate integration
test binary with its own counting `#[global_allocator]`
(`compress_into_alloc_{none,zstd}_is_zero_after_warmup`,
`compress_into_alloc_lz4_is_one_bounded_allocation_per_call_not_zero`) that
proves `None`/`Zstd` allocate nothing after their first call and documents
`Lz4`'s one-allocation-per-call floor (see "What's implemented" above).

### Row encoding tests (T-037)

`row_encode_*` tests: `tests/row_encode.rs` has
`row_encode_size_counter_matches_written` (1000-case proptest: the size
computed by `SizeCounter` always equals the bytes the write pass actually
produces — enforced live by `encode_row_body`'s `debug_assert_eq!`) and
`row_encode_out_of_order_complex_cells_match_sorted_output` (shuffled
complex-column cell input still serializes identically to already-sorted
input). `tests/row_encode_alloc.rs` uses a counting `#[global_allocator]`
to assert zero allocations serializing rows into an already-warm writer, for
single-column clustering, multi-column clustering, and a complex column —
driven through `SSTableWriter::serialize_rows_for_test` (`#[doc(hidden)]`),
which isolates row-body encoding from `add_partition`'s Partitions.db
key-trie/bloom-filter insert (real, but not this packet's scope, and not a
constant cost per call — see the module doc for the measurement it took to
rule that path out).

### Row encoding: size-then-write, no per-row allocation (T-037)

`SSTableWriter::serialize_row` no longer builds a `row_body: Vec<u8>` scratch
buffer per row. The row-body serializers (`push_unsigned_vint_to`,
`write_columns_subset`, `write_complex_deletion`, `serialize_cell`) are
generic over a `RowSink` trait (`put`/`put_byte`); `encode_row_body` runs
once against a `SizeCounter` (counts bytes, allocates nothing) to learn the
row body's length, writes the size vints, then runs the identical function
again against the real `DataBuffer`. A `debug_assert_eq!` catches any
divergence between the two passes. `split_u16_prefixed` (multi-column
clustering) is an iterator, not a `Vec<&[u8]>`.

**Complex-column element order.** Cassandra requires a complex column's
element cells in cell-path order. Two of ferrosa-storage's cell-producing
paths already guarantee this — `merge.rs::merge_rows` (cross-source/
compaction merge) explicitly re-sorts by `(col_idx, path)`, and
`memtable/sharded.rs::merge_row_into_partition` (read-modify-write) inserts
via a `(col_idx, path)`-keyed binary search — but a **freshly inserted**
row's cells come from `ferrosa-row-bridge::collection::build_collection_cells`,
which emits elements in the CQL value's wire order (relying on the driver to
have pre-sorted a `Set`, and doing no path-order sort at all for a `Map` or
`List`) and can reach a brand-new partition (`rows: vec![row]`) without going
through either merge path. So `encode_row_body` does not trust the input:
it clears and refills the writer's reusable `complex_order_scratch: Vec<usize>`
(indices into `row.cells`, sorted by path) for each complex-column run instead
of assuming order and `debug_assert`-ing it — the scratch is allocated once
and reused across rows and runs, never per row.

This work also fixed a real allocation bug it exposed: `crate::marshal::
collection_value_type` built a throwaway `Vec<&str>` (via `top_level_args`)
on every call; since `encode_row_body` runs twice per row, that doubled an
existing per-row allocation into two. `top_level_args` is now an
allocation-free iterator (`ferrosa-sstable/tests/row_encode_alloc.rs`
`RE3` caught this via a counting `#[global_allocator]`).

## Specs

- [Architecture overview](specs/overview.md) — module map, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + scope gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later

## Metadata allocation bounds (T-081)

File-backed writers retain one aligned segment for each Partitions.db, Rows.db
and uncompressed CRC.db output. They never accumulate complete encoded indexes
or CRC tables. The trie keeps only its active key frontier: branch descriptors,
completed sibling pointers, the previous key, and at most one page of encoded
node scratch. The initial frontier covers 64-byte keys (65 descriptors and
16,640 child slots); longer keys can grow depth/key storage, and unusually deep
branching can grow the child arena. These allocations depend on live key shape,
not completed table output, and remain reusable. Key bounds and row-column
scratch similarly retain their largest observed shape. Bloom's existing fixed
10,000-key estimate allocates its bitset once; this packet changes serialization,
not Bloom sizing or false-positive semantics.

Open initializes Crossbeam's cached Context and selector capacity on both pump
threads. It also exercises the producer's real timed park path before writes
begin, because a zero-duration select does not initialize that path. The
`FERROSA_SSTABLE_PUMP_WAIT_WARMUP_TIMEOUT_MS` setting bounds this one-time
startup wait (default 1 ms, valid 1–100 ms); invalid values log an error and
fall back to the default. Built-in abort signals only disconnect a one-slot
channel, avoiding rendezvous select packets while preserving wakeups for every
receiver. The unchanged 64 MiB row test measures zero allocations; existing
pump allocation and cancellation gates remain unchanged. The compressed
steady-state gate still detects one Rayon external-job injection allocation
per measured run; that scheduling issue remains separate from metadata
serialization.

`CompressionInfo::read` rejects negative signed fields, non-positive chunk
lengths, inconsistent chunk counts, and offset lists that are not zero-based
and strictly increasing. It validates the encoded option/offset extents before
walking them or allocating the offset vector. The `fuzz/` package keeps
`sstable_offsets` and `checksum_parsing` targets seeded from the checked-in
golden SSTables; the offset target pairs mutated metadata with the golden
Data.db and compares any accepted chunk reads with the golden decoded bytes.

The process-start writer tuning variables, configurable maxima, defaults, and
allocation guards are listed in [Runtime tuning](#runtime-tuning).

A schema's explicit `compression.chunk_length_kb` setting takes precedence over
the chunk-size environment variable. Larger chunks reduce chunk and index
operations while increasing read amplification. File-format values such as
CRC width, offset encoding, chunk ordering, and row serialization flags remain
fixed for compatibility.
Test-support pump hooks are scoped by output directory and wrap each real sink
once at open. Integration tests can record effective segment/depth/mode or inject
backpressure without process environment changes; guards unregister on drop.
Production builds do not include the hook registry or its locks.

### Pump wiring acceptance (T-045)

Fixed component labels count pump opens by I/O mode, successful physical bytes (including padding), and sink write requests. The counters use atomics and resolve labels once per file; no per-write allocation or registry lock is added. Scoped test traces preserve real file I/O and detect known legacy component-write routes.

File-backed flush and compaction publication compare the producer's `Digest.crc32`
with a digest recomputed from staged `Data.db` bytes before promotion. This check
is unconditional; `FERROSA_COMPACTION_VERIFY_OUTPUT` controls only the separate
row/partition count walk.

## Backpressure coverage (T-041)

A path-scoped test sink wraps the actual file writer. The bounded `WriteGate`
fixture retains counters and permits, never payload bytes. Tests gate each
component for all three codecs and compare all output bytes against the
ungated writer, gate compression independently of Data.db, pin the ring at
`depth + 1` owned segments, and check the final CompressionInfo header patch.
A gated file source through the real `ReadAheadReader` proves a blocked
producer stops pulling after its current window plus one prefetch. A deliberate
60-second stall is in `::slow::`; release resumes progress without loss/reorder.

Invalid pump environment settings log ERROR once and retain safe defaults.
Valid alignment normalization logs configured/effective sizes at WARN. A
flusher I/O failure observed through a disconnected free channel now preserves
the original device cause. Cancellation cannot interrupt an arbitrary device
syscall: shutdown joins after that call returns or the controlled gate releases.

The isolated `backpressure_memory` integration binary measures live heap, RSS,
and ungated/resumed real-file throughput. Its measurements apply to the pump;
queue/memtable counters alone are not evidence of engine-wide RSS or throughput.

Read-ahead payload buffers are now reused by moving the old window through the
existing request channel. Two buffers support current data plus one prefetch;
both are bounded at open, including random-seek/error/cancel paths.

The strict repeated-backpressure gate measured 96 free-segment waits across 32
three-segment submissions into a two-segment ring: zero allocation events and
zero allocated bytes (0 bytes/wait). This supersedes the earlier T-034 residual
park-allocation claim for the measured built-in `NeverAbort` path. Custom abort
implementations and transport/read-ahead allocation surfaces remain separate.
