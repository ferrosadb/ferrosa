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

- **Complex-column support (FMEA ST-3).** Implement collections / UDT / tuple /
  frozen cell encode+decode in the Data.db codec, or surface unsupported complex
  columns as an explicit error to any consuming crate that needs them.
- **Snappy / Deflate compression.** Currently only None / LZ4 / Zstd are
  supported. Add the remaining Cassandra algorithms behind the `Compression`
  enum for broader fixture compatibility.
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
