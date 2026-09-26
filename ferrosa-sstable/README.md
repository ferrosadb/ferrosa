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
  `FrozenType(..)`).
- **Trie index** — on-disk trie walker + builder (`trie/`) backing the partition
  index (Partitions.db) and the row index (Rows.db) for wide clustered
  partitions.
- **Compression** — `Compression::{None, Lz4, Zstd { level }}` with per-chunk
  CRC32 validation on read. `compress_bound(len)` / `compress_into(src, dst)`
  (T-036) compress into a caller-owned buffer instead of returning a fresh
  `Vec`, for the write pump's `ChunkCompressor` (T-038) to preallocate once
  and reuse per chunk. Into-buffer API per codec: `None` is a plain
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
| `pump` | `PumpConfig` — runtime tunables for the aligned write pump (`FERROSA_SSTABLE_WRITE_SEGMENT_BYTES`, `FERROSA_SSTABLE_WRITE_QUEUE_DEPTH`), from T-030. **T-032 adds**: `SegmentSink` (`pwrite`/`sync_data`/`set_len`/`fadvise_dontneed`/`mode`, `pub(crate)`) — the seam every SSTable component write will go through; `FileSink`, the production implementation (opens with `direct::open_bypassing`'s flags plus the `dio_align` block probe); and `AlignedPump`, a synchronous (`depth = 0`) writer — one `AlignedBuf` segment allocated at `open`, one `SegmentSink::pwrite` per full segment (never a remainder shuffle, D5), `finish` pads/syncs/trims and returns the exact logical length, `digest()` exposes the producer-side `Digest.crc32` (T-011's `DigestCrc32`), and `Drop` without `finish` WARNs if anything was written. `direct::DirectWriter` is now a thin wrapper over it. Behind the `test-support` feature (and always under `cfg(test)`): `RecordingSink`, `FaultySink` (11 scripted fault kinds), `GateSink` (permit-gated, timeout-bounded) — all `pub(crate)` today; no cross-crate entry point exists until a later packet (T-033/T-038) makes `AlignedPump`/`SegmentSink` reachable from outside this crate. `depth >= 1` (a background flusher thread over bounded channels) is T-033 |
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

## Public API (key entry points)

| Area | Items |
|------|-------|
| I/O traits | `ReadAt`, `WriteAt`, `FileReadAt`, `FileWriteAt` |
| Direct I/O | `direct::DirectWriter` (page-cache-bypassing sequential writer: O_DIRECT/`F_NOCACHE`), `direct::DirectReadFile`, `DirectMode`, `direct_write_{fallbacks,files,bytes}_total`, `direct_read_{fallbacks,files,bytes}_total` |
| DIO alignment probe | `dio_align::{resolve_block, probe, block_for, ProbeResult, TooLarge, MAX_BLOCK}`, `dio_align::dio_align_probe_fallbacks_total` |
| Write pump tunables | `pump::PumpConfig` (`from_env`, `effective_segment`), `pump::{SEGMENT_BYTES_ENV, QUEUE_DEPTH_ENV}` |
| Scan / read-ahead | `scan::ReadAheadReader::{new, with_prefetch}`, `FileReadAt::{open_scan, is_scan}`, `scan::parse_scan_window` |
| Reader | `SSTableReader::{open, get_partition, get_clustering_row, may_contain_key, partitions_iter, seek_to_token, salvage, validate_data_extent, load_crc_table, load_digest, verify_digest}`, `SSTableComponents` |
| Writer | `SSTableWriter::{new, new_file_backed, add_partition, finish, finish_to_directory}`, `WriteOptions`, `SSTableOutput`, `SSTableOutputFiles` |
| Checksums | `checksum::{DigestCrc32, ChunkCrc, ChunkCrcTable, digest_bytes, format_digest, parse_digest, compute_chunk_crc}` |
| Types | `Partition`, `Row`, `LivenessInfo`, `DeletionTime`, `Compression` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `Token`, `DecoratedKey`, `PartitionKey`, `CellValue`,
  Murmur3 hashing, `Error`/`Result` (the shared types the format encodes).

External: `crc32fast`, `libc` (O_DIRECT/`F_NOCACHE`/`posix_fadvise` for
`direct::DirectWriter`), `lru`, `lz4_flex`, `memmap2`, `rayon`, `tracing`
(loud fallback logging), `zstd`. **No async runtime** — positional I/O is
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
