---
crate: ferrosa-sstable
doc: roadmap
last_updated: 2026-06-19
---

# ferrosa-sstable — Roadmap

Sourced from in-code deferral notes (`data.rs`, `writer.rs`), the FMEA gaps
([fmea.md](fmea.md)), the existing topic spec, and the dependency/usage review.

## Now (highest value)

- **Close the range-tombstone gap (FMEA ST-2).** The reader silently skips range
  tombstone markers and the writer never emits them, so range deletes are
  invisible through BTI. Either implement encode/decode for range tombstone
  markers, or add a **fail-loud guard** so the engine cannot route a range
  delete through this path undetected. Highest-RPN correctness item.
- **Assert token order at the writer boundary (FMEA ST-9).** `add_partition`
  documents but does not enforce its token-order precondition. Add a
  debug-assert (or cheap monotonic-key check) so out-of-order input fails loudly
  in tests instead of producing a silently corrupt trie.

## Next

- **Wire the aligned write pump (sstable-write-pump plan, T-032 onward).**
  Done so far: `pump::PumpConfig` (T-030), `dio_align::{resolve_block, probe,
  block_for}` (T-031; gnu and musl alike via a raw `SYS_statx` syscall against a
  hand-rolled kernel-UAPI struct, FMEA ST-13), `Compression::compress_into`
  (T-036) and allocation-free size-then-write row encoding over a generic
  `RowSink` (T-037). Still to do: the `SegmentSink`/flusher-thread pump
  (T-032/T-033) and `DataSink` (removing `Data.raw`, T-038), which replaces the
  `DataBuffer` that row encoding still writes through. See
  `ferrosa-suite/specs/sstable-write-pump/`.
- **Complex-column support (FMEA ST-3).** Implement collections / UDT / tuple /
  frozen cell encode+decode in the Data.db codec, or surface unsupported complex
  columns as an explicit error to any consuming crate that needs them.
- **Snappy / Deflate compression.** Currently only None / LZ4 / Zstd are
  supported. Add the remaining Cassandra algorithms behind the `Compression`
  enum for broader fixture compatibility.
- **`Lz4` chunk compression cannot reach zero allocations (T-036 finding).**
  `compress_into`'s `Lz4` arm calls `lz4_flex::block::compress_into`, which
  allocates a fresh boxed match-finding hash table every call — the pinned
  `lz4_flex = "0.11"` exposes no reusable-state entry point (the private
  `compress_internal`/`HashTable` types would need to be reused, and aren't
  `pub`). `None` and `Zstd` are zero-allocation after warm-up
  (`tests/compress_into_alloc.rs`); `Lz4` is a bounded, constant one
  allocation per call. Worth revisiting when T-038's `ChunkCompressor` lands
  (an upgraded `lz4_flex`, or a small vendored patch exposing a reusable
  hash table, would close the gap) — low priority since it's a bounded,
  non-scaling cost, not a leak.
- **Bloom filter sizing.** `SSTableWriter::new` sizes the bloom filter for a
  fixed 10 000-key default with a "production would resize" note. Make the size
  derive from the actual partition count (builder pattern or post-hoc resize) so
  the FP rate holds for large tables.

## Later

- **Big-format (legacy `*-big-*`) read support.** Out of scope today (ADR-004,
  BTI-only). Revisit only if importing legacy Cassandra Big-format SSTables
  becomes a requirement; until then `open` fails loudly on Big format rather
  than misreading.
- **Broaden salvage coverage.** Extend `salvage` / `SalvageStats` with
  index-corruption recovery (today it relies on the partition-index walk for
  boundaries) so a damaged Partitions.db can still be partially recovered.

## Non-goals

- Async / S3 I/O — lives in `ferrosa-storage` behind the `ReadAt`/`WriteAt`
  traits; this crate stays synchronous and runtime-free.
- CQL planning, schema DDL, or cluster routing — belong to the calling crates.
