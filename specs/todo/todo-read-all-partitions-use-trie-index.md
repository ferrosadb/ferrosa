---
type: enhancement
priority: P2
created: 2026-04-06
updated: 2026-04-06
---

# read_all_partitions should use the BTI trie index for speed

## Description

`SSTableReader::read_all_partitions()` currently starts at byte 0 and sequentially parses the entire Data.db file. It does not consult the partition index (BTI trie) at all.

For large SSTables this is suboptimal — the trie already has the offset of every partition, so a trie-guided iteration could:
- Skip directly to each partition's start offset (no sequential parsing overhead)
- Validate that the trie offsets are consistent with the data file length
- Enable parallel partition reads for large files

## Current Code

`ferrosa-sstable/src/reader.rs:205-219` — sequential loop from offset 0.

## Suggested Approach

Add a `partition_offsets()` iterator to `PartitionIndex` that yields `(DecoratedKey, u64)` pairs from the trie in token order. Use these offsets in `read_all_partitions` instead of sequential parsing.
