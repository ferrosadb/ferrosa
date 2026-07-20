# ferrosa-sstable

> The Cassandra-compatible **BTI SSTable** reader/writer — the crate's on-disk
> data layer. Reads and writes the 7-component BTI (Big Trie-Indexed) format
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
  all 7 components (Data.db, Partitions.db, Rows.db, Filter.db,
  CompressionInfo.db / CRC.db, Statistics.db, TOC.txt) either as in-memory
  buffers (`SSTableOutput`) or staged files (`SSTableOutputFiles`). A
  self-readback verification pass (`WriteOptions::verify_output`, default on)
  reopens the finished table and checks the partition count.
- **BTI read** — `SSTableReader` opens a table from component handles and serves
  point lookups (`get_partition`, `get_partition_limited_rows`,
  `get_clustering_row`), bloom/bounds pre-checks (`may_contain_key`), and
  streaming iteration in token order (`partitions_iter` → `PartitionIter`) with
  token-seek (`seek_to_token`) and projection variants.
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
  CRC32 validation on read.
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
  on the `complex_collections` flag (`SSTableWriter`/`DataReader::with_complex_collections`),
  default `false` = Ferrosa's legacy whole-value storage; Cassandra import opts in.
  Still deferred: **tuple** complex columns, and a *persisted* format version so
  Ferrosa's own complex writes and legacy whole-value SSTables coexist on one read
  path (the flag is a stopgap — t_b7cec413). Tracked in t_83c4f093.
- **Snappy / Deflate compression** — only None / LZ4 / Zstd are supported.

## How it works

| Module | Responsibility |
|--------|----------------|
| `io` | `ReadAt`/`WriteAt` positional traits, `FileReadAt`/`FileWriteAt`, bounded block cache (`CachedReadAt`) |
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
| Reader | `SSTableReader::{open, get_partition, get_clustering_row, may_contain_key, partitions_iter, seek_to_token, salvage, validate_data_extent}`, `SSTableComponents` |
| Writer | `SSTableWriter::{new, new_file_backed, add_partition, finish, finish_to_directory}`, `WriteOptions`, `SSTableOutput`, `SSTableOutputFiles` |
| Types | `Partition`, `Row`, `LivenessInfo`, `DeletionTime`, `Compression` |

## Dependencies

**Calls** (ferrosa crates this depends on):

- **`ferrosa-common`** — `Token`, `DecoratedKey`, `PartitionKey`, `CellValue`,
  Murmur3 hashing, `Error`/`Result` (the shared types the format encodes).

External: `crc32fast`, `lru`, `lz4_flex`, `memmap2`, `rayon`, `zstd`. **No async
runtime** — positional I/O is synchronous; S3 wrappers live in `ferrosa-storage`.

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

## Specs

- [Architecture overview](specs/overview.md) — module map, data flow, invariants
- [FMEA / known issues](specs/fmea.md) — failure modes + scope gaps
- [Roadmap](specs/roadmap.md) — Now / Next / Later
