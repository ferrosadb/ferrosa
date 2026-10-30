# 022 — Ferrosa-native SSTable format (`fnf`) with recoverable framing, beside a BTI export path

- Status: proposed
- Date: 2026-10-29
- Supersedes: none
- Related: ADR-004 (BTI-only, Big format deferred), `specs/lean4/Ferrosa/Storage/{Recovery,Tombstone}.lean`

## Context

`ferrosa-sstable` writes and reads the Cassandra 5.x **BTI** format. That choice buys
compatibility and a very compact, very fast index, and it also inherits three properties that
are invisible until a machine dies:

1. **No checksum over any index structure.** The only CRCs in the format are per compressed
   chunk and `CRC.db` over the data extent. `Partitions.db`, `Rows.db` and the trie blocks
   have none. A flipped byte in an internal trie node does not fail to parse; it redirects
   every key under that subtree, and the walker still returns `Ok`.
2. **The index is an authority, not an accelerator.** `sstable-verify` compares the footer's
   `key_count` against a linear walk of `Data.db`, but only in one direction
   (`reachable < expected` errors). A trie that lost entries while `Data.db` stayed intact
   passes, and lookups then silently miss partitions. Cassandra's own answer is
   `sstablescrub`, an offline O(N) tool (CASSANDRA-9591 made it work with `Index.db` *missing*,
   on the argument that data is the authority and the index only an accelerator — a property
   the format does not enforce and the reader does not have).
3. **A torn file yields nothing.** The trie's root and footer are written at close, so a
   partially written `Partitions.db` does not open. Data is still salvageable only because
   `salvage` enumerates by walking `Data.db` (`next_partition_count`) rather than consulting
   the index — a deliberate exception in the reader, not a property of the file.

Two related findings from the Lean specs: the commitlog records carry no CRC either
(`Storage/Recovery.no_checksum_detects_only_truncation`), and a cell's `i32` local-deletion-time
wraps past 2038 (`Storage/Tombstone.ldt_overflows_before_2038_reachable_now`) — a bug class
Cassandra ships an `sstablescrub --reinsert-overflowed-ttl` flag for (CASSANDRA-14092).

## Decision

Add a second on-disk format, **`fnf` (Ferrosa native format)**, which is **the default for every
new table from day one**. `bti` remains as the export format (switch the table, compact, hand
the output to Cassandra) and as the ingest format (read an existing Cassandra SSTable, write
`fnf`). Selectable per table, but the default is `fnf`. It keeps
BTI's data encoding and trie index — the fast path is the reason the trie exists and it stays —
and changes the *framing*:

| | BTI | fnf |
|---|---|---|
| Data records | unframed, back-to-back | `sync:u32 \| len:u32 \| crc32c:u32 \| BTI partition body` |
| Index integrity | none | `crc32c` per 4 KiB trie block |
| Index authority | required to open | accelerator; `Data.db` is the authority |
| Torn tail | file does not open | **prefix is servable**, state is named |
| Completeness | footer present or not | `COMPLETE` / `RECOVERABLE(n)` / `TORN` |
| Resume a flush | impossible (trie is finalized) | truncate at the last verified record, continue |
| One damaged record | costs every partition after it | **costs only itself**: scan to the next `sync` word, require two consecutive accepted candidates |
| Default | — | **`fnf` for all new tables**; `bti` for export and ingest |

Backward compatibility in the *write* direction is explicitly **not** a requirement. A table
that must be readable by Cassandra is switched to `bti` and compacted; the compaction output is
BTI and Cassandra reads it. The reader accepts both formats simultaneously, dispatched on a
magic sniff, so migration and ingest work per table with no cluster-wide flag day.

### Layout

```text
fnf header  (48 B)
  magic:8 = "FRSSTB01" | version:u16 | flags:u16 | generation:u64
  | partition_count_hint:u64 | header_crc32c:u32 | pad

data region
  record* = len:u32 | crc32c:u32 | partition body   (body begins key_len:u16 | key, as in BTI)

trie index  (Partitions.db, Rows.db)
  trie blocks, each: payload | crc32c:u32          (4 KiB target block size)

fnf footer  (48 B, written last, fsync'd before publish)
  magic:8 | partition_count:u64 | min_token:i64 | max_token:i64
  | data_rolling_crc32c:u32 | index_off:u64 | index_len:u64
  | index_crc32c:u32 | footer_crc32c:u32
```

The record body is unchanged BTI, so the data codec, the bloom filter and the statistics file
are shared. `len` is redundant with the body's internal lengths **on purpose**: it is what lets
a scanner skip a damaged record without trusting anything inside it, and what lets a reader
detect a short tail (`actual < declared`) that BTI currently discovers only via
`validate_data_extent` after the fact.

### Why each property holds

- **Prefix recovery.** Nothing in a record points forward. Scanning stops at the first record
  whose `len` overruns EOF or whose CRC mismatches; everything before it is intact and
  decodable. Spec: `Format.scan_is_a_prefix`, `Format.corruption_stops_the_scan`.
- **Blast radius one partition.** The CRC covers exactly one record, so one damaged byte costs
  the record it is in. Accepted: this is the performance/robustness trade chosen deliberately.
- **The trie can never return a wrong value.** Every lookup re-reads the record at the offset
  the trie proposed, and that record carries its own key. A mis-resolution lands on a record
  whose key differs, and the lookup returns *absent* — fail-closed, not fail-wrong. Spec:
  `Format.trie_cannot_produce_a_wrong_value`. This is what makes it safe to keep the trie on
  the fast path even though its bytes are only block-level checksummed.
- **The trie is only a speed-up.** Every answer the trie gives appears in a linear scan of the
  verified prefix (`Format.trie_answer_agrees_with_the_scan`), so a damaged or missing index is
  rebuildable in O(N) rather than fatal.
- **Three states.** `COMPLETE` (footer + all CRCs), `RECOVERABLE n` (no footer, `n` verified
  records), `TORN` (header bad). `RECOVERABLE` is servable; `TORN` is quarantined, never
  deleted.
- **Flush resumption.** Because nothing is positional, a writer can reopen a partial file,
  truncate to the last verified record, and continue.

### Performance contract

The framing is designed so the fast path pays for bytes it already has:

- CRC32C over one record, computed on the buffer that is already being read into the block
  cache. With compression on, a CRC was already being computed per chunk; the record CRC
  covers the *logical* record and the chunk CRC the compressed bytes. Estimated <1% for
  typical records; **this is a claim to be measured, not asserted.**
- The key comparison needs no extra I/O: the BTI partition body already begins with
  `key_len | key`, so the check is a `memcmp` on a slice already in hand.
- Trie block CRCs are verified once when a block is first touched — the same point a
  decompression CRC is verified — so steady-state lookups pay nothing.
- No alignment padding, no per-record fsync, no change to the compaction write pattern
  (still one sequential pass).

**Gate:** point-read p99 on `fnf` must be within 5% of BTI on the same data, and compaction
throughput within 3%. If the record CRC cannot be made to fit that budget, the fallback is one
CRC per 64-partition block (blast radius 64) rather than dropping integrity.

### Streaming

Partition-based, with explicit stream **boundaries**: a sender emits bounded partition ranges
and the receiver fsyncs and acknowledges per boundary. The on-disk format never crosses the
wire, so `fnf` is free to change between versions without a rolling-upgrade protocol.

### Export to Cassandra

```
ALTER TABLE ks.t WITH sstable_format = 'bti';
nodetool compact ks t;
```

The compaction writer takes the format from table metadata; output is BTI, which Cassandra
reads. This requires `SSTableWriter` to select a format per table rather than being BTI-only —
the only structural change to the existing writer.

## Body: a hand-rolled layout

The body is **hand-rolled, not generated**. A codegen's byte output depends on its version and
on how a Builder happened to set fields; a storage format needs the opposite — the same
partition always producing the same bytes, and the layout owned by this repository. Cap'n
Proto's *layout discipline* is adopted (fixed stride, field ids, dense struct arrays, no
pointers), not its runtime. What that buys, and what is proved in `Format.lean`:

| Property | Theorem |
|---|---|
| The bytes determine the cell; no layout ambiguity | `encode_injective` |
| Metadata scans never decompress a value | `meta_reads_do_not_touch_the_arena` |
| A pruned partition never held a match | `zone_map_prune_is_sound` |
| A row lookup is bounded by its group, not the row count | `row_lookup_is_bounded_by_the_group` |
| Adding a column never renumbers existing cells | `column_ids_survive_a_new_column` |
| Every value reference lies inside the arena | `well_formed_bounds_every_value` |

### Record layout

```text
partition record
  meta block      (uncompressed, self-contained)
    key_len:u16 | key
    deletion: { markedForDeleteAt:i64, localDeletionTime:u64 }
    row_count:u32 | cell_count:u32
    zone: { minClustering:8B, maxClustering:8B, minTs:8B, maxTs:8B, nullCount:u32 }
    row_offsets: u32[]              one per 16 rows, into the cell list
  bitmap          u32 per 32 cells; bit n set iff cell n is an exception
  cells           cell_count × 12 B, fixed stride   (the common cell)
  exceptions      exception_count × 16 B, in cell order
  values          value arena (compressed as one block; LZ4/Zstd)
```

**The common cell, 12 bytes**, in this exact order, no padding:

```text
col:u16 | flags:u8 | reserved:u8 | valOff:u32 | valLen:u32
```

**The exception, 16 bytes**, only for cells that deviate from the row baseline:

```text
ts:i64 | ttl:u32 | ldt:u32
```

`flags` carries tombstone / expiring / empty-value. `valLen == 0xFFFFFFFF` marks a tombstone
with no value. The row's own timestamp and `ldt` live once, in the meta block; a cell that
inherits them (every cell written by the same statement — the overwhelming majority) costs 12
bytes, and a cell's exception slot is `popcountUpTo bitmap n`, one `popcount` per lookup
(`Format.popcountUpTo_monotone` — the slot map is order-preserving, so slots never collide).

A flat fixed-width cell would have to carry `ts:i64` in every cell, paying 8 bytes for a value
that is identical across the row: 28 bytes against 12. `Format.two_tier_never_costs_more` is
the exact comparison — `12c + 16e ≤ 28c` precisely when `e ≤ c`, so two tiers are never worse
and are strictly cheaper as soon as one cell shares its row's timestamp. For a ten-cell row of
64-byte values that is ~120 B of cells against ~100 B delta-encoded, rather than ~280 B.

Fixed stride is still the whole point: cell *n* is at `cells + n * 12`, so a one-column
projection out of a fifty-column row touches one cell instead of decoding fifty
(`projection_beats_sequential_for_wide_rows`). A hand layout is also 12 bytes where a generated
struct rounds up to its word size.

### Meta/values split

The meta block is uncompressed and self-contained: column ids, timestamps, liveness, row
offsets and the zone map. Only the value arena is compressed. So a scan that only needs
timestamps, a liveness check, a tombstone test, or a zone-map prune pays no decompression at
all (`meta_reads_do_not_touch_the_arena`). The record CRC covers the whole logical record,
compressed or not, so the recovery properties in the first half of this ADR are unaffected.

### Row offsets every 16 rows

4 bytes per 16 rows is a quarter-byte per row rather than 4, and a clustering lookup scans at
most 16 cells (`row_lookup_is_bounded_by_the_group`). Above ~16 rows this replaces `Rows.db`;
the row trie is kept only for very wide partitions, where a linear group scan would stop being
a constant.

### Schema component

`Schema.db` holds the table's field ids and types, versioned, and the file header records a
**schema fingerprint** so a file written under a different schema is refused rather than
misread. Cells carry field ids, not positions, so adding a column is additive and never rewrites
an existing SSTable (`column_ids_survive_a_new_column`). This is the biggest structural
improvement over `SerializationHeader`, whose positional column indices force a cluster-wide
schema agreement before a new column can be written.

### Columnar sidecar (`Columns.db`)

Built at compaction for tables that ask for it. Per column chunk: dictionary-encoded values,
min/max, null count, distinct-count-by-dictionary, and per-chunk statistics. It exists to serve
the aggregate surface from metadata instead of by walking rows:

- `COUNT(*)` from row-group counts, not from counting rows.
- `MIN`/`MAX`/`COUNT(col)` from chunk statistics.
- `COUNT(DISTINCT col)` exactly, when the dictionary is not truncated — which turns the
  exact/estimate split from `Storage/Cql/Aggregate.lean` into a *choice* the planner can make
  from the file's own metadata.
- Projection pruning for analytical scans, complementing the row-oriented fast path.

It is a **sidecar, not a second copy of the data**: column chunks reference the same arena, so
the added disk cost is the dictionary encoding plus statistics, not the values. Cost: extra
compaction work and a second write pass for tables that opt in.



- `ferrosa-sstable` gains a format dimension: `Format::{Bti, Fnf}` on read (sniffed) and on
  write (per table). `SSTableWriter` and `SSTableReader` become format-generic behind the
  existing traits; the data codec, bloom filter and statistics stay shared.
- ~8 bytes, 4–6% per record, plus 4 bytes per 4 KiB of trie. Accepted: the space is cheaper
  than the recovery.
- `salvage` stops being a special case — it becomes the normal scan path, since it is already
  index-independent.
- Startup gains a three-state decision instead of open/fail.
- `sstable_format` is per table and recorded in schema; changing it affects new SSTables only,
  so the transition is per table plus a compaction. Default is `fnf`; `bti` is used to export.
- Because `fnf` is native on day one, the BTI **export** path (compaction writes BTI) and the
  BTI **ingest** path are load-bearing from the start rather than nice-to-have: without export
  there is no escape hatch to Cassandra, and without ingest there is no way to adopt an
  existing dataset. Both are red items in `Format.lean`.

## Alternatives rejected

- **Fix BTI in place.** Would require the bytes to change; the whole point of keeping BTI is
  that Cassandra reads it. And the fixes are format-level (framing, footer), not reader-level.
- **One file per format, no trie (sparse index only).** Rejected: the trie is the fast path, and
  the record-key comparison already makes a corrupt trie fail closed, so the trie's risk is
  bounded without giving up its speed.
- **Checksum only the data extent (`CRC.db`-style).** Detects corruption but names no record,
  so the blast radius is the whole file and the prefix is unservable. Worse than nothing for
  the recovery case.
- **Full per-leaf trie verification on the read path.** O(depth) extra reads per lookup to
  re-derive keys; the record-key comparison achieves the same guarantee for free.
