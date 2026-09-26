---
crate: ferrosa-sstable
doc: fmea
last_updated: 2026-09-26
---

# ferrosa-sstable — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate is the on-disk format layer on the engine's
critical read/write path, so corruption and compatibility failures carry high
severity — a wrong byte here is silent, durable data loss.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| ST-1 | BTI encoding diverges from Cassandra 5.x byte layout | SSTables written here are unreadable by Cassandra (or vice-versa); silent corruption across the compat boundary | 10 | 2 | 6 | 120 | `tests/cassandra_compat.rs` is a binary-exact oracle against fixtures generated from the Cassandra submodule; trie/VInt/byte-comparable have dense unit tests. |
| ST-2 | Range tombstone markers in a Data.db row stream | Reader silently skips them; deletes within a range are not reflected on read (incorrect results) | 9 | 3 | 7 | 189 | **Known scope gap.** Writer never emits them; reader skips. Documented in `data.rs`/`writer.rs`. Must be tracked so the engine does not rely on range deletes through this path. See roadmap. |
| ST-3 | Complex columns (collections, UDT, tuple, frozen) written/read | Deferred codec mishandles or drops complex cell data | 8 | 3 | 6 | 144 | **Known scope gap.** Data.db codec handles simple cells only; complex columns deferred. Surface to callers that need them rather than silently degrading. |
| ST-4 | Big-format (legacy `*-big-*`) SSTable presented to the reader | No Big-format read path exists; the table cannot be opened | 6 | 2 | 3 | 36 | **Out of scope by design** (ADR-004, BTI-only). Open fails loudly rather than misreading; not a silent corruption. |
| ST-5 | Corrupt on-disk length prefix drives a huge allocation | A bogus multi-TB varint length OOMs the process | 9 | 2 | 2 | 36 | `MAX_VALUE_LEN` (256 MiB) hard ceiling rejects oversized buffers before allocating; covered in `data.rs`. |
| ST-6 | Data.db truncated by a non-atomic flush (index claims N, fewer reachable) | Partitions silently missing on read | 9 | 2 | 4 | 72 | `validate_data_extent` compares index `key_count` vs walkable partitions and errors loudly; `verify_output` self-readback (default on) catches the count mismatch at write time. |
| ST-7 | Intra-partition parse drift / bitmap under-count corruption | One bad row cascades and loses the rest of the table | 9 | 2 | 4 | 72 | `salvage` decodes each partition independently at its indexed offset (no cross-partition cascade), returns `SalvageStats` with partial/complete counts. |
| ST-8 | Compressed chunk CRC mismatch or non-monotonic offsets | Silent bit-rot read as valid data | 9 | 2 | 3 | 54 | Per-chunk CRC32 validated on read; chunk-offset monotonicity and decompressed-length bounds checked in `read_compressed_chunk`. |
| ST-9 | Partitions added out of token order to the writer | Corrupt partition trie → wrong/missing lookups | 9 | 2 | 6 | 108 | Documented precondition (`add_partition` requires token order). Not currently asserted at the API boundary — a debug-assert on monotonic keys would lower detection cost. |
| ST-10 | `seek_to_token` resident index scales with partition count | Repair Merkle scan over a multi-GB table OOMs | 8 | 2 | 3 | 48 | Fixed: `build_token_summary` downsamples to a hard `PARTITION_TOKEN_SUMMARY_MAX_ENTRIES` (65 536) ceiling; small tables keep a full stride-1 index. |
| ST-11 | Bloom filter false-positive / hash mismatch vs Cassandra | Extra Data.db reads (perf) or, if hashes diverge, missed keys | 7 | 2 | 5 | 70 | Cassandra-compatible double-hashing over Murmur3 `h1`/`h2` from `ferrosa-common`; FP rate tunable via `WriteOptions::bloom_fp_chance`. |
| ST-12 | Uncompressed Data.db had no checksum at all (stale bytes / hole / bit flip with the right length reads as valid) | Silent bit-rot or stale-segment corruption undetectable until a decode error, or never | 9 | 3 | 8 | 216 | **Fixed (T-011).** `Digest.crc32` (every table) and `CRC.db` (uncompressed tables, per-chunk CRC32) are computed while Data.db is written (`checksum` module). **Residual gap:** verification is opt-in per reader (`load_digest`/`load_crc_table`) — wired into `ferrosa-storage`'s flush-open helpers (`flush.rs::{open_file_sstable, open_reader_from_paths}` and `engine.rs::open_sstable_from_dir`), but a caller that constructs `SSTableReader::open` directly and never calls the loaders (e.g. a future new caller, `ferrosa-ctl`, `ferrosa-sstable-dump`) reads unchecked with no error. Compaction's own content verification (M2/M3 in `publication-safety.md`) and the R5 scrub pass are separate, not-yet-built consumers of these checksums (tracked as T-012/T-038 in `compiled-project-plan.md`). |
| ST-13 | `dio_align::probe` risked compiling to the always-`Unsupported` stub on ferrosa's production musl binary | A `*-unknown-linux-musl` build (`make build-musl`, the shipped Linux target) would never get a real `STATX_DIOALIGN` reading, so the write pump would always use the safe `MIN_BLOCK` (4096) floor even on hardware with a larger true alignment — never wrong, only unable to use the probed value | 3 | 10 | 2 | 60 | **Resolved: raw syscall (T-031).** `libc` 0.2.186 gates the typed `statx` struct's `stx_dio_*` fields, the `statx` FFI declaration, and `STATX_DIOALIGN` behind `cfg(any(gnu, android, all(musl, musl_v1_2_3)))`, where `musl_v1_2_3` depends on the *building* machine's detected musl version, not the target triple — so a `libc::statx`-based probe would have been gnu-only in practice. `dio_align::probe` instead defines its own `#[repr(C)]` `KernelStatx` matching the stable kernel UAPI (`include/uapi/linux/stat.h`, compile-time size/offset asserts) and calls it via a raw `libc::syscall(libc::SYS_statx, …)`, using only the unconditionally-available `libc::syscall`, `libc::SYS_statx` (per-arch), and `libc::AT_EMPTY_PATH`. The probe now runs identically on gnu and musl; `Unsupported` (old kernel, `ENOSYS`) is still counted in `dio_align_probe_fallbacks_total`, so a real gap stays observable rather than silent. |

## Top risks to act on

1. **ST-2 (RPN 189) — range tombstone skip.** The reader silently drops range
   tombstone markers, so range deletes are invisible through this path. This is
   the highest-RPN item because the effect is *incorrect query results*, not a
   loud failure. Either implement range tombstones or make the engine guarantee
   it never routes range deletes through BTI, and add a fail-loud guard.
2. **ST-3 (RPN 144) — complex columns.** Collections/UDT/tuple are deferred;
   any consumer that needs them must not silently get degraded data.
3. **ST-1 (RPN 120) — BTI byte compatibility.** Severe but well-mitigated by the
   binary-exact Cassandra oracle; keep the fixture suite green on every change.

## Detection assets

- `tests/cassandra_compat.rs` — binary-exact round-trip vs Cassandra fixtures.
- `tests/property_tests.rs` — proptest round-trips over the codec surface.
- `tests/p0_production_disk_replay.rs` — real on-disk replay regression.
- `validate_data_extent` + `WriteOptions::verify_output` — truncation/partial-write guards.
- `salvage` / `SalvageStats` — best-effort recovery + observability on corrupt tables.
- `tests/oracle.rs` + `tests/golden/` (T-035) — a checked-in golden corpus
  froze today's writer output byte-for-byte, ahead of the write-pump rewrite
  (`ferrosa-suite/specs/sstable-write-pump/`) that changes how `writer.rs`
  produces `Data.db`. `oracle_golden_reproduction` catches any byte drift
  that rewrite introduces; `oracle_file_backed_matches_in_memory` (1000
  proptest cases) catches the writer's two entry points diverging from each
  other.
- `checksum_` prefixed tests in `checksum.rs`, `writer.rs`, `reader.rs` (T-011) —
  digest/CRC.db format proptests, writer↔`crc32fast` cross-checks across chunk
  boundaries, a flipped-bit-in-Data.db regression naming the failing chunk
  offset, and an old-SSTable-without-components-still-reads regression.
