# Storage Engine Coverage Report

> Generated: 2026-04-18
> Zone: `ferrosa-storage/` + `ferrosa-sstable/`
> Method: code audit against specs/

---

## 1. Feature Inventory

### Memtable

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| M1 | `Memtable` trait | Abstract put/get/snapshot/size interface enabling backend swap | `ferrosa-storage/src/memtable/mod.rs:1` |
| M2 | `ShardedBTreeMemtable` | 64-shard BTreeMap with CAS counters; default backend | `ferrosa-storage/src/memtable/sharded.rs:1` |
| M3 | `SkiplistMemtable` | Lock-free skiplist alternative (feature flag) | `ferrosa-storage/src/memtable/skiplist.rs:1` |
| M4 | `MemIndex` (secondary index memtable) | In-memory secondary index updated on every put | `ferrosa-storage/src/memtable/mem_index.rs:1` |
| M5 | `EagerIndex` | Eager per-flush index materialization | `ferrosa-storage/src/memtable/eager_index.rs:1` |
| M6 | `VectorIndex` (memtable HNSW) | In-memory vector index for ANN queries | `ferrosa-storage/src/memtable/vector_index.rs:1` |

### Commit Log

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| C1 | `CommitLog` | CAS-allocated WAL with segment lifecycle management | `ferrosa-storage/src/commitlog/mod.rs:1` |
| C2 | Segment CAS allocator | `AtomicU64` in-flight writer counter + CAS position allocation | `ferrosa-storage/src/commitlog/segment.rs:1` |
| C3 | Segment lifecycle | New → active → closed → discarded; `discard_completed()` per checkpoint | `ferrosa-storage/src/commitlog/mod.rs:1` |
| C4 | `Mutation` binary format | Self-describing big-endian: keyspace/table/key/token/rows/cells | `ferrosa-storage/src/commitlog/mutation.rs:1` |
| C5 | Sync strategies (3) | `BatchSync`, `PeriodicSync` (10ms default), `GroupSync` (1ms) | `ferrosa-storage/src/commitlog/sync.rs:1` |
| C6 | Segment replay / crash recovery | Sync-marker chain traversal via `SegmentReader` | `ferrosa-storage/src/commitlog/reader.rs:1` |
| C7 | `CommitLogCheckpoint` | Per-table flush tracking; format_version JSON | `ferrosa-storage/src/commitlog/checkpoint.rs:1` |
| C8 | `SegmentDescriptor` | Segment file metadata (version, id, compression) | `ferrosa-storage/src/commitlog/descriptor.rs:1` |
| C9 | `CommitLogArchiver` | S3 upload of closed segments for PITR; SHA-256 checksum + archive-manifest CAS | `ferrosa-storage/src/commitlog/archiver.rs:39` |
| C10 | `CdcReader` | Change-data-capture reader over segments; checkpoint save | `ferrosa-storage/src/commitlog/cdc.rs:85` |
| C11 | Oversized-entry handling | Returns `EntryTooLarge` error rather than panic or silent drop | `ferrosa-storage/src/commitlog/mod.rs` (segment rotation path) |
| C12 | Commit-log manifest | Per-log manifest tracking archived segment inventory | `ferrosa-storage/src/commitlog/manifest.rs:1` |

### Flush Path

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| F1 | `FlushTarget` trait | Abstract flush backend (memory vs file) | `ferrosa-storage/src/flush.rs:1` |
| F2 | `FileFlushTarget` | tmp-then-rename atomic file write + fsync; monotonic generation counter | `ferrosa-storage/src/flush.rs` |
| F3 | `InMemoryFlushTarget` | Test-only in-memory flush | `ferrosa-storage/src/flush.rs` |
| F4 | Flush size check | `flush_if_needed()` threshold-based auto-flush | `ferrosa-storage/src/engine.rs:475` |
| F5 | Gate A — clustering-shape validation | Validates every row's clustering bytes before writing Data.db | `ferrosa-sstable/src/writer.rs:317` |
| F6 | Gate B — defensive self-readback | `verify_output_readable()` reopens freshly written SSTable via full reader pipeline | `ferrosa-sstable/src/writer.rs:376` |

### Compaction

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| K1 | `CompactionStrategy` trait | Pluggable per-table compaction strategy | `ferrosa-storage/src/compaction/strategy.rs:1` |
| K2 | STCS | Size-tiered strategy: buckets by median ratio; `min_threshold`/`max_threshold` env config | `ferrosa-storage/src/compaction/strategy.rs:71` |
| K3 | UCS | Unified compaction: density = size/token_share; levels by fan_factor; DDL-selectable | `ferrosa-storage/src/compaction/strategy_ucs.rs:1` |
| K4 | `CompactionExecutor` | Dedicated `std::thread` + channel; submit + poll; readback verification | `ferrosa-storage/src/compaction/executor.rs:1` |
| K5 | Compaction readback | Post-compaction partition/row count check; aborts on mismatch | `ferrosa-storage/src/compaction/executor.rs:291` |

### SSTable Format (ferrosa-sstable)

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| S1 | `SSTableWriter` (BTI) | In-memory accumulation; writes Data.db, Partitions.db, Rows.db, Filter.db, CompressionInfo.db, Statistics.db, TOC.txt | `ferrosa-sstable/src/writer.rs:133` |
| S2 | `SSTableReader` (BTI) | Point-lookup via bloom + trie; `get_partition()`, `read_all_partitions()` | `ferrosa-sstable/src/reader.rs:65` |
| S3 | Bloom filter | Cassandra-compatible Murmur3 double-hashing; read + write | `ferrosa-sstable/src/bloom.rs:23` |
| S4 | LZ4 compression | Via `lz4_flex`; default; 16KB chunks | `ferrosa-sstable/src/compression.rs:24` |
| S5 | Zstd compression | Via `zstd`; configurable level | `ferrosa-sstable/src/compression.rs:24` |
| S6 | BTI partition index (trie) | On-disk trie over byte-comparable keys; walker + page-aware builder | `ferrosa-sstable/src/partition_index.rs:42` / `trie/` |
| S7 | BTI row index (trie) | Per-partition trie over clustering separators; granularity 16KB | `ferrosa-sstable/src/row_index.rs:49` |
| S8 | Statistics.db | 4-component (Validation, Compaction, Stats, SerializationHeader) with CRC32 | `ferrosa-sstable/src/statistics.rs:33` |
| S9 | Byte-comparable key encoding | OSS50 escape encoding; token sign-bit flip | `ferrosa-sstable/src/byte_comparable.rs:53` |
| S10 | Varint encoding | Cassandra leading-ones prefix; signed zigzag | `ferrosa-sstable/src/varint.rs:34` |
| S11 | Range tombstone marker skip | Gracefully skips IS_MARKER rows during Data.db read | `ferrosa-sstable/src/data.rs` |
| S12 | `ferrosa-sstable-dump` CLI | Reads and dumps SSTable contents to stdout | `ferrosa-sstable/src/bin/ferrosa-sstable-dump.rs:1` |
| S13 | `ferrosa-sstable-import` CLI | Imports data into SSTable format | `ferrosa-sstable/src/bin/ferrosa-sstable-import.rs:1` |

### S3 Write-Behind

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| W1 | `UploadManager` | Bounded tokio mpsc; exponential backoff retry (5×, 100ms base); uploads SSTable components | `ferrosa-storage/src/upload/manager.rs:1` |
| W2 | `ObjectStoreConfig` | 12-factor env config for S3-compatible endpoint | `ferrosa-storage/src/upload/config.rs:1` |
| W3 | SHA-256 upload integrity | Computed on upload, stored as `x-amz-meta-ferrosa-checksum`; verified on read | `ferrosa-storage/src/upload/manager.rs` |
| W4 | `PendingUploadsLog` | Crash-recovery log of in-flight uploads; add/remove/pending_entries | `ferrosa-storage/src/upload/pending_log.rs:28` |
| W5 | `Manifest` | JSON + etag-based CAS via `PutMode::Update`; `format_version` field | `ferrosa-storage/src/manifest.rs:1` |
| W6 | Index file upload (`UploadTask::IndexFiles`) | Sidecar index files uploaded with same retry/integrity semantics | `ferrosa-storage/src/upload/manager.rs` |

### NVMe Pinning

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| N1 | `PinConfig` / `PinMode` | Table-extension-driven pin mode: `None` or `NvMe` | `ferrosa-storage/src/pin_config.rs:1` |
| N2 | NVMe skip-upload | Pinned tables skip S3 submission; SSTable pinned in `LocalCache` | `ferrosa-storage/src/engine.rs` (flush + compaction paths) |
| N3 | Pin max_bytes enforcement | Oldest pinned SSTables evicted from pinned set when cap exceeded | `ferrosa-storage/src/cache.rs` |
| N4 | ALTER TABLE pin/unpin | Unpin enqueues existing SSTables for S3; pin cancels pending uploads | `ferrosa-storage/src/engine.rs` |
| N5 | Pin observability metrics | `pinned_tables`, `pinned_bytes`, `pin_evictions_total` | `ferrosa-storage/src/metrics.rs` |

### Write Verify (2026-04-19 incident gates)

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| V1 | Gate A — `validate_clustering_shape` | Pre-write clustering column count/size check per row | `ferrosa-sstable/src/writer.rs:317` |
| V2 | Gate B — `verify_output_readable` | Post-write self-readback gated by `WriteOptions.verify_output` (default `true`) | `ferrosa-sstable/src/writer.rs:376` |

### S3 Bootstrap / Download

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| B1 | `LocalCache` LRU | LRU eviction with pinned set; `touch()` / `evict_if_needed()` | `ferrosa-storage/src/cache.rs:1` |
| B2 | S3 fetch-on-miss | Cache miss falls through to S3 download via `ReadAt` S3 impl | `ferrosa-storage/src/store.rs` (S3ReadAt in read path) |
| B3 | S3 bootstrap | Load manifest from S3, populate local cache at startup | `ferrosa-storage/src/engine.rs` |

### Snapshot / PITR

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| P1 | `SnapshotManager` | Create/list/delete/load snapshots; metadata-only (copies manifest + schema JSON) | `ferrosa-storage/src/snapshot/manager.rs:38` |
| P2 | `SnapshotMetadata` | format_version, name, created_at, expires_at, commit_log_position, node_id, ephemeral | `ferrosa-storage/src/snapshot/metadata.rs:1` |
| P3 | Snapshot GC safety | `all_referenced_sstable_ids()` for orphan-cleanup coordination; `cleanup_expired()` | `ferrosa-storage/src/snapshot/manager.rs:196` |
| P4 | `RestoreManager` | Load+validate snapshot, download SSTables, download archived segments | `ferrosa-storage/src/restore/manager.rs:21` |
| P5 | Restore validation | `restore/validation.rs`: SHA-256 verification of downloaded segments | `ferrosa-storage/src/restore/validation.rs:1` |
| P6 | PITR engine E2E tests | 6 `upload_manifest_for_test`-backed integration tests in engine.rs | `ferrosa-storage/src/engine.rs:6101` |

### Legacy Corruption Quarantine

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| Q1 | Startup quarantine | Zero-byte/missing component files moved to `{table_dir}/quarantine/` on load | `ferrosa-storage/src/engine.rs:1280` |
| Q2 | Quarantine logging | ERROR per quarantined SSTable; continues loading non-corrupt SSTables | `ferrosa-storage/src/engine.rs:1322` |

### Timeseries / Accord / Observability (storage-layer)

| # | Feature | Description | Source |
|---|---------|-------------|--------|
| T1 | `TimeSeriesAggregator` | Ring-buffer consolidation for time-series tables | `ferrosa-storage/src/timeseries/aggregator.rs:40` |
| T2 | `StorageStatsTable` virtual table | `system_observability.storage_stats`; memtable/SSTable/S3/compaction metrics | `ferrosa-storage/src/virtual_tables.rs:1` |
| T3 | `SecondaryIndexesVirtualTable` | `system_views.secondary_indexes`; per-index build state and metrics | `ferrosa-storage/src/index/virtual_table.rs:1` |
| T4 | Accord `WriteGate` | `check_write_gate` / `check_write_gate_range` routes non-transactional writes through Accord if key conflicts exist | `ferrosa-storage/src/accord/write_gate.rs:37` |
| T5 | `SubscriptionObserver` | `WriteObserver` impl for CQL SUBSCRIBE; async, ref-counted per-table | `ferrosa-storage/src/subscription_observer.rs:1` |
| T6 | `BatchlogStore` | Storage-layer batchlog for multi-partition batch durability | `ferrosa-storage/src/batchlog.rs:1` |

---

## 2. Spec Coverage Matrix

| Feature(s) | Spec that covers it | Coverage quality |
|---|---|---|
| M1-M3 ShardedBTreeMemtable, Memtable trait | `specs/storage.md` §Memtable Trait / ShardedBTreeMemtable | **Full** |
| M4-M6 MemIndex, EagerIndex, VectorIndex | `specs/secondary-index-pipeline.md` (index scheduler), `specs/storage.md` §Index Support | **Partial** — memtable index types not individually speced |
| C1-C8 CommitLog, CAS allocator, lifecycle, sync, mutation format | `specs/storage.md` §CommitLog | **Full** |
| C9 CommitLogArchiver | `specs/pitr.md` §CommitLogArchiver | **Full** |
| C10 CdcReader | None | **Missing** |
| C11 Oversized entry | `specs/storage.md` §Commit Log Oversized Entry Handling | **Full** |
| C12 Commit log manifest | `specs/pitr.md` (archive-manifest.json schema) | **Partial** — only the PITR manifest; internal per-log manifest not documented |
| F1-F4 FlushTarget, FileFlushTarget, flush size check | `specs/storage.md` §FlushTarget / Flush Path | **Full** |
| F5-F6 Gate A + Gate B | `specs/ARCHITECTURE.md` §Invariants (added 2026-04-19) | **Partial** — ARCHITECTURE.md has 2 paragraphs; no dedicated spec; not in `specs/storage.md` or `specs/sstable.md` |
| K1-K3 STCS, UCS, CompactionStrategy trait | `specs/storage.md` §Compaction + `specs/ucs-compaction-architecture.md` | **Full** |
| K4 CompactionExecutor | `specs/storage.md` §CompactionExecutor | **Full** |
| K5 Compaction readback | None | **Missing** — only referenced in `specs/ARCHITECTURE.md` by implication |
| S1-S11 SSTable format (BTI writer, reader, all components) | `specs/sstable.md` | **Full** |
| S12-S13 CLI tools (dump, import) | `specs/sstable.md` §Phase 2 (deferred) | **Stale** — tools exist in `src/bin/` but spec says Phase 2 deferred |
| W1-W3 UploadManager, integrity | `specs/storage.md` §S3 Upload Manager | **Full** |
| W4 PendingUploadsLog | `specs/archive/todo-pending-uploads-no-crash-recovery.md` (TODO) | **Partial** — implemented; todo spec not updated to reflect done |
| W5 Manifest CAS | `specs/storage.md` §Manifest | **Full** |
| W6 Index file upload | `specs/storage.md` §Index Support §UploadTask::IndexFiles | **Full** |
| N1-N5 NVMe pinning | `specs/archive/nvme-pinning-architecture.md` + `specs/storage.md` §NVMe Table Pinning | **Full** |
| V1-V2 Gate A/B write-verify | `specs/ARCHITECTURE.md` only | **Partial** — not in `specs/sstable.md` or `specs/storage.md`; no standalone spec |
| B1-B3 LocalCache, S3 fetch-on-miss, bootstrap | `specs/storage.md` §LocalCache; `specs/decisions/001-write-behind-s3.md` | **Partial** — S3 fetch-on-miss not documented; bootstrap listed as "follow-on" in storage.md but implemented |
| P1-P6 SnapshotManager, RestoreManager, PITR E2E | `specs/pitr.md` + `specs/decisions/011-s3-native-pitr.md` | **Partial** — spec describes API design but E2E test infrastructure (`upload_manifest_for_test`) and restore validation are not mentioned |
| Q1-Q2 Startup quarantine | `specs/implemented/bug-startup-quarantine-corrupt-sstables.md` | **Partial** — bug spec only; not reflected in `specs/storage.md` as an invariant |
| T1 TimeSeriesAggregator | `specs/archive/analysis/fmea-rrd-timeseries.md` | **Partial** — FMEA exists, no architecture spec |
| T2-T3 Virtual tables (storage_stats, secondary_indexes) | `specs/storage.md` §StorageStatsTable + §Index Support | **Full** |
| T4 Accord WriteGate | `specs/ARCHITECTURE.md` §Consensus + accord test specs | **Partial** — not in `specs/storage.md` |
| T5 SubscriptionObserver | `specs/storage.md` §SubscriptionObserver | **Full** |
| T6 BatchlogStore | `specs/archive/bolt-compat-testing.md` (side ref); `specs/in-process/gap-S1-register-batchlog-handlers.md` | **Missing** — no storage-level batchlog spec |
| jemalloc allocator | `specs/threat-model.md` T-D3; `specs/ARCHITECTURE.md` §Binary (startup note) | **Partial** — fragmented across threat model and ARCHITECTURE.md; no dedicated ops runbook |

---

## 3. Gaps

1. **[P0] Gate A + Gate B have no canonical spec.** The 2026-04-19 incident aftermath added `validate_clustering_shape` and `verify_output_readable` to `writer.rs`. These are load-bearing correctness invariants. They are mentioned in two paragraphs of `specs/ARCHITECTURE.md` but are absent from `specs/sstable.md` and `specs/storage.md`. A future maintainer changing the flush path has no spec to cross-check against. **Action:** Update `specs/sstable.md` §SSTableWriter and `specs/storage.md` §Flush Path with Gate A/B semantics, failure modes, and the invariant that `verify_output = true` in production.

2. **[P0] Legacy-corruption quarantine is not an invariant in storage.md.** The startup quarantine (Q1/Q2) is implemented in `engine.rs:1280` and documented only in a bug spec (`specs/implemented/bug-startup-quarantine-corrupt-sstables.md`). The FMEA (`fmea.md` OPS-3) identifies "S3 manifest references locally quarantined files → 404 storm" as an open risk requiring a quarantine marker in S3 — this mitigation is unspecced and unimplemented. **Action:** Add a §Startup Quarantine section to `specs/storage.md` covering the `{table_dir}/quarantine/` mechanic, the OPS-3 S3-marker mitigation, and the open gap on quarantine-to-S3 coordination.

3. **[P1] `specs/storage.md` §Not Yet Implemented is stale.** The document lists "S3 upload wiring," "Manifest CAS loop," "Recovery (`open()`)," and "S3 integrity verification" as unimplemented. All four are now implemented (UploadManager wired, CAS loop in manifest.rs, open_from_snapshot in engine.rs, SHA-256 in upload manager). Leaving these as "not yet implemented" will mislead contributors into re-implementing or skipping them. **Action:** Update `specs/storage.md` §Follow-on Work to reflect current implementation status. Add PITR restore path (`open_from_snapshot`, `RestoreManager`, `PendingUploadsLog`, `CommitLogArchiver`) to the implemented section.

4. **[P1] `PendingUploadsLog` and `CdcReader` are unspecced.** Both are implemented components with durable persistence (disk log for pending uploads, checkpoint for CDC) that affect crash-recovery and S3 consistency guarantees. Neither appears in `specs/storage.md` or any dedicated spec. The pending-uploads todo spec (`specs/archive/todo-pending-uploads-no-crash-recovery.md`) is missing from the archive and its implementation status is unclear. **Action:** Add `PendingUploadsLog` to `specs/storage.md` §S3 Upload Manager and document crash-recovery semantics. Add `CdcReader` to a new §Change Data Capture subsection.

5. **[P1] `specs/sstable.md` CLI tools section is stale (Phase 2 "deferred").** `ferrosa-sstable-dump` and `ferrosa-sstable-import` exist in `ferrosa-sstable/src/bin/` but `specs/sstable.md` §Phase 2 still marks them as deferred. This is misleading. **Action:** Move dump/import from Phase 2 to Phase 1 in `specs/sstable.md` with a one-line status note.

6. **[P2] jemalloc is not documented in an ops runbook.** The allocator swap is in `ferrosa/Cargo.toml` and `ferrosa/src/main.rs`, mentioned in `specs/threat-model.md` T-D3 and `specs/in-process/bug-read-path-memory-growth-bloats-coordinator.md`, but there is no section in any storage or operations spec covering: how to dump profiles (`jeprof`/SIGUSR2), how to tune arenas under cgroup pressure, or what alerts to set. **Action:** Add a §Allocator subsection to `specs/storage.md` or a new `specs/ops-memory.md` covering jemalloc configuration, profiling, and the arena-fragmentation failure mode.

7. **[P2] TimeSeriesAggregator has no architecture spec.** The crate has 4 files (aggregator, consolidation, config, ring) totalling ~2700 lines but only an FMEA for the feature. There is no design document covering ring-buffer eviction, consolidation window semantics, or the late-data handling path. **Action:** Create `specs/timeseries-architecture.md` from the existing implementation.

8. **[P2] Compaction executor readback (K5) is undocumented.** The post-compaction partition/row count verification in `compaction/executor.rs:291` is a silent correctness invariant with no spec backing. **Action:** Add a §Compaction Readback paragraph to `specs/storage.md` alongside the existing Gate A/B update (Gap 1).

---

## 4. Recently-Changed Invariants (2026-04-19 Incident Aftermath)

| Invariant | Implemented | In specs/ | Notes |
|---|---|---|---|
| **Gate A — clustering-shape validation** (`validate_clustering_shape`, `writer.rs:317`) | Yes — `project-plan-2026-04-blueprint-sprint.md` records "(done 2026-04-19)" | **No** — only in `specs/ARCHITECTURE.md` §Invariants (2 paragraphs); absent from `specs/sstable.md` and `specs/storage.md` | P0 gap — see Gap 1 |
| **Gate B — defensive self-readback** (`verify_output_readable`, `writer.rs:376`) | Yes — same source | **No** — same as Gate A | P0 gap — see Gap 1 |
| **jemalloc as global allocator** (`ferrosa/src/main.rs:37`) | Yes | Partial — `specs/threat-model.md` T-D3 mitigated, `specs/ARCHITECTURE.md` §Binary mentions it | No ops runbook; P2 gap — see Gap 6 |
| **Legacy-corruption quarantine** (`engine.rs:1280`, commit `6aaf56d`) | Yes — `specs/implemented/bug-startup-quarantine-corrupt-sstables.md` + `specs/fmea.md` FMEA-STG-04 reference | **Partial** — bug spec only; not an invariant in `specs/storage.md`; OPS-3 S3-marker mitigation unimplemented | P0 gap — see Gap 2 |

**Summary:** All four invariants are implemented in code. None are fully reflected in the primary architecture specs (`specs/storage.md`, `specs/sstable.md`). Gate A/B and the quarantine invariant are the most urgent to document because they guard against silent data corruption.

---

## 5. Recommendations

1. **Update `specs/sstable.md`** — Add a §SSTableWriter Invariants section covering Gate A (`validate_clustering_shape`), Gate B (`verify_output_readable`), the `WriteOptions.verify_output` flag behavior, and what error type each gate raises. Move CLI tools from Phase 2 to Phase 1. (~300 words, no new file needed.)

2. **Update `specs/storage.md`** — (a) Replace the entire §Not Yet Implemented table with an accurate status table. (b) Add §Startup Quarantine invariant covering the `{table_dir}/quarantine/` mechanic and the open OPS-3 S3-marker gap. (c) Add §PendingUploadsLog and §CdcReader subsections. (d) Add a compaction readback paragraph. This is the single highest-leverage edit — one file that closes Gaps 1 (partial), 2, 3, and 4. (~600 words added.)

3. **Create `specs/write-correctness-invariants.md`** — Standalone reference covering the full chain of correctness gates: Gate A (clustering shape) → Gate B (self-readback) → compaction readback → startup quarantine → S3 SHA-256 verify. Include the incident timeline (2026-04-17 zero-byte Rows.db, 2026-04-19 gates added) so future contributors understand why these gates exist and what breaks if they are disabled. (~500 words, new file.)

4. **Create `specs/ops-memory.md`** — Covers jemalloc configuration (`unprefixed_malloc_on_supported_platforms`), arena-fragmentation failure mode, container cgroup interaction, `jeprof` profiling via SIGUSR2, and the alert threshold for RSS growth. Reference the in-process bug (`bug-read-path-memory-growth-bloats-coordinator.md`) as context. (~400 words, new file.)

5. **Update `specs/pitr.md`** — Add a §Implementation Status section noting what is implemented vs. the original spec: `open_from_snapshot`, `upload_manifest_for_test` test helper, `restore/validation.rs` SHA-256 verification, `CommitLogArchiver` actual API diff from spec (no `poll_interval` field on the struct), and the fact that timestamp-filtering replay is partially implemented. (~200 words added.)
