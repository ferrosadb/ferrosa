---
crate: ferrosa-storage
doc: fmea
last_updated: 2026-07-12
---

# ferrosa-storage — FMEA / Known Issues

Failure modes ranked by **RPN = Severity × Occurrence × Detection** (1–10 each;
higher = worse). This crate is the durable substrate, so most severities are at
the top of the scale: a defect here is silent data loss or corruption.

| ID | Failure mode | Effect | S | O | D | RPN | Mitigation / status |
|----|--------------|--------|---|---|---|-----|---------------------|
| ST-1 | **Durability window under the default `Periodic` sync strategy** — writes acked before fsync are lost on power-loss/crash | Up to `sync_interval` (default 10 ms) of acked writes lost on an unclean stop; replay cannot recover what was never fsynced | 9 | 4 | 6 | 216 | **Designed window, not a bug.** `BatchSync` gives zero loss at a latency cost; `Group` bounds it. macOS uses `F_FULLFSYNC` by default (`macos-fullfsync`) because plain fsync is not a hardware flush. Document the window per deployment; choose `Batch` for zero-loss tables. |
| ST-2 | **Compaction drops/duplicates/reorders rows** vs. its inputs | Silent permanent data loss or resurrection of deleted data | 10 | 2 | 5 | 100 | Compaction validator (oracle + differential row-equivalence) gates outputs; `compaction-validator` feature exports it to the loadgen soak; `compaction_property.rs` proptest. Residual risk under UCS density edge cases. |
| ST-3 | **SSTable / S3 object corruption read back as valid data** | Wrong query results or a crash mid-scan | 10 | 2 | 5 | 100 | SHA-256 integrity metadata verified on S3 read; `write_verify` self-readback after flush; corrupt-SSTable detector + `scan_table_dir_for_corrupt` → quarantine. Residual: silent bit-rot on local NVMe between flush and first read. |
| ST-4 | **S3 write-behind backlog → local disk fills before upload drains** | Local NVMe ENOSPC; flushes stall; writes fail closed | 8 | 3 | 5 | 120 | `local_disk_free_reserve_bytes` admission check fails writes closed *before* the commit-log append; bounded upload mpsc applies backpressure; pending-upload log replays flat and generation-directory SSTable components after crash. Residual: no proactive disk-pressure-driven flush throttle beyond the reserve gate. |
| ST-5 | **Pinned (NVMe) table loses its only copy** — cache eviction or pin/unpin races delete the sole local copy with no S3 backup | Permanent data loss for a pinned table | 9 | 2 | 6 | 108 | Manifest-pinned entries are never LRU-evicted; pin→unpin enqueues S3 upload before relying on remote durability; `pin_max_bytes` evicts oldest from the pinned set only (still LRU-eligible, still local). Residual: pinned tables have no remote copy by design — node loss = data loss. |
| ST-6 | **Malformed row wedges flush or replay** (e.g. truncated timeuuid from `now()`) | Without quarantine, the flush task or node crashes in a loop | 9 | 2 | 4 | 72 | **Mitigated**: rows failing cell/clustering validation are quarantined to `quarantine/*.jsonl`; flush continues. `FLUSH_QUARANTINED_ROWS_TOTAL` non-zero is a steady-state alert (means a write bypassed the `Memtable::put` guard). |
| ST-7 | **Commit-log replay OOM** — unbounded pending mutations buffered when no schema is loaded | Node OOM on crash recovery | 8 | 2 | 5 | 80 | `max_pending_replay_mutations_without_schema` caps the legacy no-schema buffer and fails closed; normal recovery preloads `schema.json` and streams replay into registered tables. |
| ST-8 | **Reader-pool fan-in blow-up under full token overlap** (repair digests) | Memory growth → OOM under the fmem cgroup | 7 | 3 | 5 | 105 | Engine-wide LRU reader pool caps resident readers (`sstable_reader_cache_cap`, default 256); compaction acquires a `CompactionGate` permit; range/digest walks stream one partition per source. Residual: strict open-reader bound under *fully* overlapping repair remains a separate acceptance gate, not enforced in-engine. |
| ST-9 | **Manifest CAS conflict loses an SSTable from the live set** | An uploaded SSTable is not advertised → invisible data after restart | 8 | 2 | 5 | 80 | Etag-based `PutMode::Update` conditional put; conflict → reload + retry. Residual: verify retry is bounded and surfaces a loud error rather than silently dropping the entry. |
| ST-10 | **Snapshot / PITR restore applies an inconsistent point** | Restored database missing or duplicating writes around the cut | 9 | 2 | 6 | 108 | Restore manager + `restore/validation.rs` validate the snapshot + archived commit-log segments before applying; archiver ships segments to S3. Residual: end-to-end PITR coverage is still maturing (see suite-level active work). |
| ST-11 | **Lost write under memtable backpressure deadlock** | Sustained-write workload stalls or a second unbounded memtable grows | 7 | 2 | 4 | 56 | Synchronous in-line flush in `write()` on `memtable_backpressure_bytes`; `flush_guard` serializes so a runaway writer waits rather than growing a second memtable. `test_config` sets the threshold to `u64::MAX` so tiny-threshold tests do not deadlock. |
| ST-12 | **Uploaded-cache eviction deletes a not-yet-durable SSTable** — disk-pressure eviction treated manifest membership as proof of S3 durability and deleted the only local copy of a generation still queued for upload | Permanent loss of un-uploaded generations; after restart the manifest references them but they are absent in S3 *and* local disk → `skipping stale manifest entry` and divergent/partial table visibility across coordinators (#235, `agent_memory.typed_edges`) | 9 | 3 | 6 | 162 | **Mitigated**: `collect_uploaded_local_sstables` excludes any `(table_id, gen)` still listed in the pending-upload log (the entry is removed only after S3 confirms), and **fails closed** (evicts nothing) if the pending set cannot be read. Pairs with upload-replay finding generation-subdir components so staged SSTables actually reach S3. Residual: eviction trusts the pending-log as the durability oracle rather than a direct S3 object check — a generation dropped from the log without a confirmed upload would still be evictable. |
| ST-13 | **COUNT(*) over-count — range merger fuses overlapping SSTables into one concatenated run** — `partition_into_disjoint_runs` compared raw `smallest_key`/`largest_key` bounds as byte-comparable, but SSTables written in a Cassandra-shaped/legacy encoding store bounds that are NOT byte-comparable; mis-comparing them as token-disjoint concatenated overlapping SSTables into a single merge "run", so per-key duplicates never reached `merge::merge_partitions` | Wrong query results: `COUNT(*)` (and any metadata/row scan over the fused run) counts each row once per SSTable that holds it — per-node inflation that scales with SSTable count (observed 50807 / 27928 / 46066 for a consistent 21168-row `agent_memory.typed_edges`) | 8 | 3 | 7 | 168 | **Mitigated**: run grouping now decodes each SSTable's bounds to a `DecoratedKey` and colors intervals on decoded token order; any SSTable whose bounds fail `byte_comparable::decode` is isolated into its own singleton run (an independent heap source) so the k-way merge still groups its keys. Tests: `store::count_range_dedups_when_sstable_bounds_are_not_byte_comparable` (portable repro), `range_merger::group_by_key_isolates_undecodable_bounds_into_singleton_runs`, and the `live-infra-tests` fixture repro over the captured node SSTables. The coordinated paged-SCAN side of the same bug (rows over-DROPPED during cross-replica dedup — "Bug B") is guarded end-to-end by `ferrosa-cluster` `range_read_stream::real_typed_edges_paged_scan_delivers_every_distinct_row` (drives the real N-way merge per page over the 3-node capture; RED when the run grouping is reverted to raw bytes). Residual: the fix trusts decode-success as the disjointness gate — a decodable-but-wrong bound would still be trusted. |
| ST-14 | **Periodic S3 sync publishes an incomplete SSTable generation** — a crash-left flat generation contains only a subset of the required components | The manifest advertises an unrestorable SSTable; bootstrap later skips or fails the entry, making locally visible data absent after restore | 9 | 2 | 6 | 108 | **Mitigated**: `sync_sstables_to_s3` requires all four required components (`Data.db`, `Partitions.db`, `Rows.db`, and `Filter.db`) to be present in the collected upload set before submitting any component or publishing the manifest entry. The check is intentionally presence-only because a zero-byte `Rows.db` is valid. Tests: `engine::tests::sync_s3_rejects_generation_missing_required_rows_component`, `engine::tests::sync_s3_rejects_generation_missing_required_filter_component`, and `engine::tests::sync_s3_accepts_present_zero_byte_rows_component`. |

## Top risks to act on

1. **ST-1 (RPN 216)** — the default `Periodic` sync strategy means acked writes
   are not durable for up to `sync_interval`. This is the single most important
   thing for an operator to understand and tune per workload; it is correct by
   design but high-impact if assumed away.
2. **ST-4 (RPN 120)** / **ST-8 (RPN 105)** — local-disk and reader-pool memory
   pressure are the resource-exhaustion paths most likely to cause a node-level
   outage; both are bounded but the bounds depend on correct config + the repair
   fan-in acceptance gate landing.

## Detection assets

- Compaction validator (`compaction/validator/`, oracle + differential).
- `write_verify` post-flush self-readback; SHA-256 S3 integrity verification.
- Corrupt-SSTable detector + `quarantine_corrupt_generation` (`self_heal/`).
- Property/fuzz suites: `engine_property`, `compaction_property`,
  `commitlog_property`, `repair_fuzz` (`test-generators`/`fuzz-fileio`).
- Metrics: `FLUSH_QUARANTINED_ROWS_TOTAL`, compaction pool/running gauges, pin
  gauges, `sstable_read_errors`.
