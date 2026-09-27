---
crate: ferrosa-sstable
doc: fmea
last_updated: 2026-09-26 (T-012)
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
| ST-12 | Uncompressed Data.db had no checksum at all (stale bytes / hole / bit flip with the right length reads as valid) | Silent bit-rot or stale-segment corruption undetectable until a decode error, or never | 9 | 3 | 8 | 216 → 30 | **Fixed (T-011, then T-012).** `Digest.crc32` (every table) and `CRC.db` (uncompressed tables, per-chunk CRC32) are computed while Data.db is written (`checksum` module). T-011 wired loading into `ferrosa-storage`'s flush-open helpers only, ad hoc, one copy of the load-and-warn logic per call site. **T-012 closed the opt-in gap**: loading is centralised in `reader::{load_checksums_if_present, load_checksums_for_generation}` and every production file-backed open path now calls it — `ferrosa-storage`'s `flush.rs::open_file_sstable`, `engine.rs::open_sstable_from_dir`, the compaction executor's input open, the local index-build backend, and `ferrosa-ctl`'s `sstable` reader. `flush_files` also recomputes `Digest.crc32` from the published `.tmp` Data.db unconditionally before promote, closing the "content check can be skipped" half of this gap (`publication-safety.md` M2/M3). **Residual:** the R5 background scrub pass that would catch corruption discovered *after* publication (not at open time) does not exist yet; a caller that builds `SSTableComponents` from bytes already in memory (S3 downloads, ephemeral merge output) rather than from files still has no file to load a digest from. |
| ST-13 | `dio_align::probe` risked compiling to the always-`Unsupported` stub on ferrosa's production musl binary | A `*-unknown-linux-musl` build (`make build-musl`, the shipped Linux target) would never get a real `STATX_DIOALIGN` reading, so the write pump would always use the safe `MIN_BLOCK` (4096) floor even on hardware with a larger true alignment — never wrong, only unable to use the probed value | 3 | 10 | 2 | 60 | **Resolved: raw syscall (T-031).** `libc` 0.2.186 gates the typed `statx` struct's `stx_dio_*` fields, the `statx` FFI declaration, and `STATX_DIOALIGN` behind `cfg(any(gnu, android, all(musl, musl_v1_2_3)))`, where `musl_v1_2_3` depends on the *building* machine's detected musl version, not the target triple — so a `libc::statx`-based probe would have been gnu-only in practice. `dio_align::probe` instead defines its own `#[repr(C)]` `KernelStatx` matching the stable kernel UAPI (`include/uapi/linux/stat.h`, compile-time size/offset asserts) and calls it via a raw `libc::syscall(libc::SYS_statx, …)`, using only the unconditionally-available `libc::syscall`, `libc::SYS_statx` (per-arch), and `libc::AT_EMPTY_PATH`. The probe now runs identically on gnu and musl; `Unsupported` (old kernel, `ENOSYS`) is still counted in `dio_align_probe_fallbacks_total`, so a real gap stays observable rather than silent. |
| ST-14 | A `SegmentSink` that silently violates its whole-buffer-or-error contract (a short write not retried, a dropped/duplicated/misplaced/bit-flipped segment) | `AlignedPump::write_all`/`finish` see `Ok(())` and report success; the file's actual on-disk bytes diverge from what the producer digested, undetectably at the pump layer | 9 | 2 | 6 | 108 | **Mitigated by design, not yet wired to a live check (T-032/T-033).** `AlignedPump` computes `Digest.crc32` on the producer side, independent of what any sink reports back (`pump_sync_faulty_sink_silent_corruption_is_only_caught_by_digest_comparison` and its T-033 `pump_async_*` counterpart across depths 1–4, using the `FaultySink` fault-injection double — now exercised through `pwritev` too — prove the pump's own digest diverges from a fresh digest of what actually landed for the silent-corruption fault kinds). The production `FileSink`'s `pwrite`/`pwritev` retries short writes internally and errors on a non-block-multiple short count in direct mode, so today's only real `SegmentSink` should never hit this — but nothing yet *calls* `AlignedPump::digest()` against a readback outside tests: that's T-012 (digest verification on published bytes, unconditional in flush and compaction) and T-038 (`DataSink` wiring `Digest.crc32` into the real publication path), tracked in `compiled-project-plan.md`. |
| ST-15 | The `depth >= 1` flusher thread panics or reports an I/O error while the producer is blocked waiting for a returned segment, or is never joined | A wedged producer thread (hang) if the disconnect signal were ever missed; or a leaked OS thread if `finish`/`Drop` failed to join | 8 | 2 | 3 | 48 | **Mitigated by design (T-033).** The flusher's stack unwinds through `run_flusher`'s return (not a bare panic across the thread boundary uncaught) — `JoinHandle::join()` in `AsyncBackend::shutdown` observes a panic as `Err`, surfaced as a pump error by `finish`, and WARNed by `Drop`; a plain I/O error is sent once on the one-slot error channel AND is the flusher's own return value, so it is visible from whichever side notices first. Dropping `full_tx` (in `shutdown`, called by both `finish` and `Drop`) always disconnects the flusher's blocking `recv()`, so a join can only be slow (bounded by however long the flusher's current device call takes), never indefinite. Covered by `pump_async_failure_while_parked_returns_err_promptly_no_leaked_thread` and the `pump_sync_faulty_sink_panic_propagates`-equivalent async fault-matrix rows. **Residual gap:** no test forces a genuine panic *while the producer is concurrently parked on `free`* (only a hard I/O error is exercised in that interleaving) — the panic path is exercised only via the depth-agnostic fault matrix, not specifically against a parked producer. |
| ST-16 | First-use Crossbeam Context/selector storage and rendezvous abort select packets allocate during pump waits | Backpressure creates allocator traffic | 3 | 2 | 2 | 12 | T-081 primes cached TLS Context and selector capacity at open on producer and flusher, before accepting writes. Built-in abort signals use a fixed one-slot channel solely for disconnection, avoiding zero-channel packets. The unchanged RE5 64 MiB row gate measures zero allocations. Existing async allocation and cancellation gates remain intact; custom AbortSignal implementations retain their own allocation behavior. T-034 already removed per-wait `after()` timer channels. |

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
- `pump_sync_` prefixed tests in `pump.rs` + `tests/pump_sync_alloc.rs` (T-032) —
  block/offset/address alignment and write-count bounds across probed blocks
  {4096, 8192, 65536} × configured segments {1, 4095, 4097, 1 MiB, 1 MiB+1};
  byte identity against a real `FileSink`-backed `DirectWriter`; the ST-14
  digest-divergence property over all six silent-corruption `FaultySink`
  faults; hard-failure faults (`Eio`/`Enospc`/`FsyncFail`/`SetLenFail`/`Panic`)
  surfacing as `Err`/panic naming the path; `Drop`-without-`finish` safety; and
  a counting-allocator regression for zero allocations in the write hot path
  after warm-up.
- `pump_async_` prefixed tests in `pump.rs` + `tests/pump_async_alloc.rs`
  (T-033, completed T-034) — the ST-14 fault matrix, now the FULL 11 `Fault`
  variants (5 hard: `Eio`/`Enospc`/`Panic`/`FsyncFail`/`SetLenFail`; 6 silent)
  at depths 1–4; depth 0..=4 mode parity; BP1/BP2 bounded-in-flight-then-
  blocks; BP4 resume with 25 randomized gate schedules by default, 10 000
  under `--release`, byte-identical every time; BP5/ST-15 failure-while-parked
  and BP6/CD1 abort-while-parked, both prompt with no leaked thread; CD2/BP8
  exactly-one-stall-edge watchdog; CD3 deterministic ≥ 3-segment `pwritev`
  coalescing; CD4 park-count contention budget (`thread_local!`, isolated
  from concurrently-running tests); and the ST-16 deterministic-vs-sustained-
  vs-sustained-backpressure allocation split in `tests/pump_async_alloc.rs`
  (see ST-16's own row for what T-034 did and did not resolve there).
- `tests/pump_loom.rs` (T-034, `--features loom`-only — NOT
  `RUSTFLAGS="--cfg loom"`, which breaks `tokio`'s own `cfg(loom)`-gated code
  since this crate pulls tokio in transitively via `ferrosa-common`; found by
  running it, not assumed) — a `loom` model of the producer/flusher protocol
  (decisions.md D2/D7) behind a `loom::sync`-based channel shim (loom cannot
  instrument `crossbeam_channel`'s own internals): one producer, one flusher,
  `depth` ∈ {1, 2}, a flusher error and a flusher panic injected at every
  segment index across every interleaving `loom::model` explores (the
  injected panic is caught inside the modeled flusher's own closure, not left
  to cross loom's scheduler as a real unwind — loom does not propagate a
  cross-thread panic through `JoinHandle::join()` the way `std::thread` does,
  found by running it). Proves no deadlock, every segment token returned-to-
  `free` or dropped exactly once, and the producer always terminates. Run
  with `cargo test -p ferrosa-sstable --release --features
  test-support,loom pump_loom_`.
- `tests/pump_stress.rs` (T-034) — 64 concurrently open pumps, independently
  randomized segment/depth/byte-count/fault, inside a 60 s enforced wall
  budget. Proves every pump reports an outcome, no flusher thread outlives
  its pump (`pump::live_flusher_threads()`, new T-034 test-only
  instrumentation), and peak process allocation stays within a generous
  multiple of the sum of each pump's own `(depth + 1) * segment` — the
  bounded-ring rule under real concurrency, not just one pump at a time.

## Component output coverage (T-040)

`component_pump_` tests exercise every component with direct and buffered sinks
under None/LZ4/Zstd, and check partial-tail truncation, empty components, sync
completion and surfaced sync errors. Metadata now receives the same pump I/O
failure handling as Data.db. Golden oracle tests guard format identity. The
T-081 change removes table-sized trie/Rows/CRC output buffers from production.
`metadata_stream_` tests cover 100,000 keys without workspace growth and
multiple clustered partitions with local trie padding preserved, across every
codec. CRC and Bloom use the same encoder for memory and streamed output.
Long-key frontier growth and existing Bloom sizing remain explicit bounds;
Rayon injection remains under unchanged strict tests. Crossbeam wait storage
is initialized at open, and abort-only channels avoid rendezvous packet
allocation; payload channels and cancellation semantics are unchanged.
