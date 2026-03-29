# Correctness Sprint Plan — Single-DC Jepsen + S3 + Accord

> Created: 2026-03-28
> Status: Draft
> Focus: Single-datacenter N-node correctness validation, S3/SSTable Cassandra format verification, Accord transaction correctness under all Jepsen failure modes, compaction S3 integration, and full CQL driver compatibility.
> Predecessor: [project-plan-unified.md](project-plan-unified.md) (Accord S1–S7)

---

## Objective

Deliver a fully correct, end-to-end single-datacenter distributed database: multi-node clustering with working compaction, durable S3 write-behind across the full flush+compact+upload lifecycle, and complete CQL compatibility across all six drivers. Correctness is validated at four independent levels:

1. **Protocol correctness** — Accord transactions are linearizable under all single-DC failure modes (T1: 3-node, T2: 5-node). Knossos/Elle must report zero violations.
2. **Storage correctness** — Every byte written to ferrosa survives flush, compaction, and S3 upload, and is readable by an independent Cassandra 5.1 reader. No silent data loss, corruption, or format deviation.
3. **Application correctness** — All open bugs (BUG-021 through BUG-026) that cause silent data loss or protocol violations are closed before Jepsen runs begin.
4. **CQL compatibility** — All six drivers (Python, Go, Node.js, Java, C#, Rust) execute the full workload matrix against a live cluster without errors or behavioral divergence.

**Phase gate:** `ferrosa-jepsen run --tier standard` reports zero anomalies. Cassandra 5.1 reader CI gate passes for flush and compaction output. All six driver smoke tests pass against a 3-node cluster.

---

## Risk Register

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|-----------|
| BUG-026 collection storage blocks most Jepsen workloads | High | High | Sprint C1 blocks C4 — fix collection storage before running Jepsen |
| Pair mode P0 replication ACK causes Jepsen bank failures | High | High | Sprint C2 fixes before C4 begins |
| Firecracker provisioning not automatable on macOS dev box | Medium | Medium | Docker-based T1 fallback already in place (docker_mini_jepsen) |
| Cassandra 5.1 reader finds format deviations in BTI writer | Medium | High | Sprint C5 dedicated to format validation; early discovery is the point |
| Accord dep-wait under concurrent failures reveals new bugs | Medium | Medium | Failures are findings, not blockers — log as new BUG-### entries |
| Compaction S3 upload races with manifest update | Medium | High | Sprint C7 addresses with explicit upload-confirmation callback before manifest write |
| Driver compatibility gaps surface new CQL bugs beyond BUG-021–026 | High | Medium | Expected — log as BUG-### and fix in C8 before declaring CQL compatibility gate green |

---

## Sprint C1: Open Bug Fixes — Data Correctness Prerequisites

**Duration:** 1 week
**Gate:** All six bugs closed, all existing tests green, `cargo test` passes.

These bugs cause silent data loss or protocol violations that will produce false Jepsen failures unrelated to Accord correctness. They must be fixed before any Jepsen runs.

| # | Bug | Severity | Root Cause (summary) | Success Criteria | Tests |
|---|-----|----------|---------------------|-----------------|-------|
| C1.1 | FRSA-BUG-021: QUERY frame bind values silently ignored | HIGH | Prepared statement `?` placeholders not substituted at execution time — bind values in QUERY frame are discarded | Bind values correctly substituted in all query types (SELECT, INSERT, UPDATE, DELETE). Prepared statement round-trip with 10 bind value types passes. | `bind_values_select`, `bind_values_insert`, `bind_values_update`, `bind_values_cassandra_compat` |
| C1.2 | FRSA-BUG-022: Schema lost on binary upgrade | HIGH | Schema registry not persisted to S3; restart from new binary starts with empty schema | Schema survives binary restart. Tables registered before restart are accessible after. | `schema_survives_restart`, `schema_survives_binary_upgrade` |
| C1.3 | FRSA-BUG-023: Phonetic index lost on schema restore | MEDIUM | Index metadata not included in schema snapshot serialization | Phonetic index entries survive schema restore. SOUNDS LIKE queries return same results before/after restore. | `phonetic_index_survives_restore` |
| C1.4 | FRSA-BUG-024: PREPARE metadata missing ALTER TABLE columns | HIGH | Schema snapshot used at prepare time does not include columns added after table creation | PREPARE after ALTER TABLE includes new columns in result metadata. `result_metadata` column count matches current schema. | `prepare_after_alter_table_columns` |
| C1.5 | FRSA-BUG-025: Map bind value decoded as blob | HIGH | CQL map type deserialization reads collection header as blob header; map entries corrupted | Map-typed bind values (`map<text, int>`) round-trip correctly through prepared statements. Values match what was inserted. | `map_bind_value_roundtrip`, `map_bind_value_cassandra_compat` |
| C1.6 | FRSA-BUG-026: Collection storage read-back fails after flush | HIGH | SSTable writer encodes collection cells incorrectly (I/O error: read_exact_at: wanted 1 bytes, got 0 on read-back) | map, set, list columns survive write → flush → read cycle. Collections written via gocql prepared statements read back correctly. | `collection_map_flush_readback`, `collection_set_flush_readback`, `collection_list_flush_readback`, `collection_via_gocql_roundtrip` |

**Refactoring required:**
- Review `ferrosa-cql/src/frame.rs` execute/query path to find where bind substitution is skipped (C1.1)
- Review `ferrosa-storage/src/writer.rs` collection cell encoding (C1.6 — root cause of I/O error on read-back)
- Review schema snapshot serialization in `ferrosa-schema/src/registry.rs` (C1.2, C1.3, C1.4)

---

## Sprint C2: P0 Storage Correctness Fixes

**Duration:** 1 week
**Gate:** Three P0 hazards closed. No unsafe `cargo test` failures. Pair mode write durability test passes.

These are write durability bugs identified in the correctness hazard scan. Each can cause silent data loss that Jepsen will detect as lost writes.

| # | Hazard | File | Root Cause | Fix | Success Criteria | Tests |
|---|--------|------|-----------|-----|-----------------|-------|
| C2.1 | Pair mode write confirmed before secondary ACK | `ferrosa-cluster/src/pair/coordinator.rs:55-66` | Primary writes locally, returns `Ok(())` without waiting for replication to secondary. Secondary crash → write lost. | Wait for replication ACK (or timeout → error) before confirming to client. Add `send_with_timeout` on replication, return `Err(ReplicationTimeout)` if secondary does not ACK within deadline. | Client only sees success after secondary ACK. Jepsen kill-primary after ACK does not lose write (secondary has it). `ReplicationTimeout` metric incremented on timeout. | `pair_write_confirmed_after_secondary_ack`, `pair_write_survives_primary_crash`, `pair_replication_timeout_metric` |
| C2.2 | S3 upload crash window: manifest updated before S3 confirmed | `ferrosa-storage/src/upload/manager.rs:73-122` | SSTable components fire-and-forget to S3 queue. Manifest updated without waiting for S3 upload confirmation. Crash between flush and S3 completion → orphaned partial files, no recovery possible. | Add `upload_confirmed` callback; manifest update (and commit-log GC) only proceeds after S3 confirms all component files written. Track per-SSTable upload state in a pending-uploads log. | Crash between flush and S3 upload → recovery detects pending-uploads log, retries, then updates manifest. No orphaned SSTables. Data readable after recovery. | `s3_upload_confirmation_before_manifest`, `s3_crash_window_recovery`, `pending_uploads_log_replay` |
| C2.3 | Manifest CAS fallback silently loses concurrent updates | `ferrosa-storage/src/manifest.rs:147-155` | `cas_supported=false` path uses unconditional PUT. Two concurrent flushes → one manifest update silently lost. | Remove `cas_supported=false` fallback. Require CAS support at startup (probe during `new_with_archive_store`). Return hard error if object store does not support CAS. Document MinIO minimum version requirement. | `probe_s3_cas()` returning `false` causes startup failure with clear error. No unconditional PUT path exists. | `manifest_cas_required_at_startup`, `manifest_concurrent_flush_preserves_all_entries` |

**Refactoring required:**
- `pair/coordinator.rs`: Change `replicate_to_secondary` from fire-and-forget to awaited with deadline
- `upload/manager.rs`: Add pending-uploads ledger (append-only file, fsynced before flush completes)
- `manifest.rs`: Delete the `if !self.cas_supported` branch entirely

---

## Sprint C3: Jepsen Infrastructure Completion

**Duration:** 1–2 weeks
**Gate:** `ferrosa-jepsen run --tier smoke` executes end-to-end, produces a history file, and the Rust linearizability checker reports pass/fail. Docker cluster (3-node) provisions automatically.

| # | Task | Size | Root Cause / Gap | Success Criteria | Tests |
|---|------|------|-----------------|-----------------|-------|
| C3.1 | Wire Rust CQL driver to live session in `driver/rust_driver.rs` | M | TODO comment: "Wire to actual CQL session + workload" — driver stub does not execute any CQL | Rust driver connects to 3-node Docker cluster, executes register workload (read/write/cas), appends operations to history file | `rust_driver_connects_to_cluster`, `rust_driver_register_history_roundtrip` |
| C3.2 | Automate cluster provisioning in `orchestrator.rs` for Docker mode | M | `setup-guest.sh` and ferrosa startup not automated; tests must be manually provisioned | `ferrosa-jepsen run --tier smoke` spins up 3-container Docker cluster via `docker-compose`, waits for CQL readiness, tears down after test | `orchestrator_docker_cluster_provision`, `orchestrator_cluster_teardown` |
| C3.3 | Wire bank and LWT workloads to live CQL | M | `workload/bank.rs` and `workload/lwt.rs` stub implementations | Bank workload executes multi-account transfers. LWT patterns 1, 3, 7 (insert-if-not-exists, cas-counter, race-delete) execute against cluster | `bank_workload_executes`, `lwt_insert_if_not_exists_executes`, `lwt_cas_counter_executes` |
| C3.4 | Implement `InvariantChecker` for bank and register workloads | S | Invariant assertions (bank total balance, register every-read-is-a-write) not connected to live workload | After smoke run: bank total balance assertion checked. Register checker reports linearizable or names counterexample. | `bank_invariant_total_balance`, `register_invariant_every_read_valid` |
| C3.5 | Implement `partition-halves` and `kill-minority` nemeses for Docker mode | M | Only no-op nemesis wired; real chaos not injected | `partition-halves` uses Docker network `--link` disconnect. `kill-minority` uses `docker stop`. Both nemeses fire during smoke run without crashing the orchestrator. | `nemesis_partition_halves_docker`, `nemesis_kill_minority_docker` |
| C3.6 | Implement `clock-skew-small` nemesis (faketime via Docker) | S | Clock chaos not wired | `clock-skew-small` injects ±100ms via container `/etc/faketime.conf` or `libfaketime` preload | `nemesis_clock_skew_docker` |
| C3.7 | Smoke tier end-to-end: `ferrosa-jepsen run --tier smoke` produces report | S | Tiers not wired to actual execution path | Smoke run completes in < 10 min. Report file written. Rust checker result in exit code. Zero false positives on a healthy cluster. | `smoke_tier_end_to_end` |

---

## Sprint C4: Single-DC N-Node Jepsen Validation

**Duration:** 2 weeks
**Gate:** Standard tier passes (T1+T2, all 16 nemeses, 16 LWT patterns, all 6 drivers, low+medium concurrency). Zero Knossos violations. All invariant assertions pass.

This sprint runs the actual validation the user asked for: does Ferrosa maintain correctness guarantees under all single-DC failure modes?

### T1: 3-Node Single-DC

| # | Workload × Nemesis | Concurrency | Checker | Success Criteria |
|---|-------------------|-------------|---------|-----------------|
| C4.1 | Register × all 16 nemeses | Low (12 clients) | Rust + Knossos | Zero linearizability violations. Every read is a previous write. |
| C4.2 | Bank × all 16 nemeses | Low+Medium (12/60 clients) | Rust + Elle | Total balance invariant holds throughout. Zero G1a/G1b/G2 anomalies. |
| C4.3 | LWT patterns 1–8 × all 16 nemeses | Low | Rust | Each pattern's correctness invariant holds. `[applied]` semantics correct. |
| C4.4 | LWT patterns 9–16 × all 16 nemeses | Low | Rust | Same. Batch CAS all-or-nothing. SERIAL consistency reads are never stale. |
| C4.5 | All 16 LWT patterns × all 6 drivers | Medium | Rust | Cross-driver invariants identical. No driver-specific linearizability failures. |

### T2: 5-Node Single-DC

| # | Workload × Nemesis | Concurrency | Checker | Success Criteria |
|---|-------------------|-------------|---------|-----------------|
| C4.6 | Register × all 16 nemeses | Low+Medium | Rust + Knossos | Zero violations. Fast/slow quorum paths both correct. |
| C4.7 | Bank × kill-majority nemesis | Low | Elle | Bank correctly reports unavailability (not silent loss) when majority killed. Recovers after restart. |
| C4.8 | LWT patterns 1–4 × partition-ring | Medium | Rust + Knossos | Partial connectivity handled correctly — Accord degrades gracefully or errors, never silently commits wrong value. |

**Failure mode coverage matrix** (each cell = "pass" or logged bug report):

```
Workload       | partition | kill-min | kill-maj | pause | clock-small | clock-large | slow-net | disk-slow | disk-fail
register       |     ✓     |    ✓     |    ✓     |   ✓   |      ✓      |      ✓      |    ✓     |     ✓     |    ✓
bank           |     ✓     |    ✓     |    ✓     |   ✓   |      ✓      |      ✓      |    ✓     |     ✓     |    ✓
lwt-insert     |     ✓     |    ✓     |    ✓     |   ✓   |      ✓      |      ✓      |    ✓     |     ✓     |    ✓
lwt-cas        |     ✓     |    ✓     |    ✓     |   ✓   |      ✓      |      ✓      |    ✓     |     ✓     |    ✓
lwt-batch      |     ✓     |    ✓     |    ✓     |   ✓   |      ✓      |      ✓      |    ✓     |     ✓     |    ✓
```

Any cell that is not "pass" becomes a bug report (BUG-### in `bugs/`). The sprint is complete when every cell in T1 is "pass" and T2 critical cells (register, bank, lwt-insert) are "pass" or have a known-issue logged.

**Refactoring expected:**
- `ferrosa-cluster/src/coordinator/read.rs`: Fix CL override not enforced on public `coordinate_read()` (BUG-012 was "fixed" per STATUS.md commit a69fcdd, but the hazard scan found residual issues — verify and patch if needed)
- `ferrosa-cluster/src/pair/coordinator.rs`: Batch atomicity — per-mutation forwarding must be atomic or fail-all (from P1 hazard: batch not atomic in pair mode)
- `ferrosa-storage/src/commitlog/segment.rs`: Crash recovery idempotency — verify duplicate replay prevention

---

## Sprint C5: S3/SSTable Cassandra Format Validation

**Duration:** 1–2 weeks
**Gate:** A Cassandra 5.1 reader (Docker container) can read every SSTable written by ferrosa for every cell type. CI test `cassandra_sstable_reader_validation` passes.

| # | Task | Size | Success Criteria | Tests |
|---|------|------|-----------------|-------|
| C5.1 | Add Cassandra 5.1 reader container to `tests/sstable-compat/` | S | Docker Compose target starts Cassandra 5.1, imports ferrosa-written SSTable, queries data | `cassandra_reader_container_starts` |
| C5.2 | SSTable round-trip: simple types (int, text, boolean, uuid, timestamp, bigint, float, double, blob) | S | Cassandra reads same values as ferrosa wrote. Byte-for-byte comparison of serialized values. | `sstable_compat_simple_types` |
| C5.3 | SSTable round-trip: collection types (map, set, list) | M | Fix BUG-026 TTL/collection encoding root cause in `ferrosa-sstable/src/writer.rs`. Cassandra reads map/set/list correctly. | `sstable_compat_collections` |
| C5.4 | SSTable round-trip: TTL cells (USING TTL) | M | Cassandra reads TTL metadata, respects expiry time. TTL countdown is correct to within 1s. | `sstable_compat_ttl_cells` |
| C5.5 | SSTable round-trip: tombstones (DELETE) | S | Cassandra sees deleted rows as deleted. No ghost reads. | `sstable_compat_tombstones` |
| C5.6 | SSTable round-trip: clustering keys, multi-partition, composite PK | M | Multi-row partition reads correctly. Clustering order preserved. | `sstable_compat_clustering_keys` |
| C5.7 | SSTable round-trip: S3 path (write → S3 upload → Cassandra reads from S3 path) | M | Cassandra reads SSTable fetched from S3. Tests the full write-behind path, not just local disk. | `sstable_compat_s3_roundtrip` |
| C5.8 | Property tests: all cell types, 1000 iterations each | M | `proptest`: random values for all 8 simple types, map/set/list, TTL, tombstone all round-trip. No panic on any input. | `property_simple_types_roundtrip`, `property_collections_roundtrip`, `property_ttl_roundtrip` |
| C5.9 | Add C5 tests to CI (nightly or PR gate) | S | `tests/sstable-compat/` runs on every PR. Cassandra reader failure is a CI failure. | CI workflow update |

**Refactoring required:**
- `ferrosa-sstable/src/writer.rs`: Fix TTL cell serialization (FRSA-BUG-026 root cause)
- `ferrosa-sstable/src/writer.rs`: Fix collection cell encoding (map/set/list header bytes)
- `ferrosa-sstable/src/data.rs`: Verify tombstone encoding matches Cassandra BTI spec
- Document any intentional format deviations in `specs/sstable.md`

---

## Sprint C6: Accord Transaction Correctness Under All Jepsen Failures

**Duration:** 2 weeks
**Gate:** For each of the 16 nemeses in T1+T2, a dedicated Accord correctness assertion (beyond linearizability) passes: dep-wait ordering, crash recovery idempotency, batch atomicity, no phantom reads. Zero new bugs logged that are Accord protocol violations (as opposed to infrastructure bugs).

| # | Correctness Property | Failure Mode | How to Validate | Tests |
|---|---------------------|--------------|----------------|-------|
| C6.1 | Commit log replay idempotency | kill-minority, disk-fail | Kill node mid-write, restart, verify no duplicate rows in recovered state. Count of applied writes = count of ACKed writes. | `commitlog_replay_idempotent_after_kill`, `commitlog_no_duplicate_rows` |
| C6.2 | Dep-wait ordering: transactions execute in dependency order | partition-halves + concurrent writes | Write txn T1 depends on T2. Inject partition. Verify T1 never applies before T2's effects are visible. | `dep_wait_ordering_under_partition` |
| C6.3 | Batch atomicity in pair mode and cluster mode | kill-primary, partition-one | Multi-row CQL BATCH: kill coordinator after first row written. Verify all rows committed or none — no partial batches. | `batch_atomicity_kill_coordinator`, `batch_atomicity_partition` |
| C6.4 | Recovery coordinator activates correctly | kill-majority (then revive) | Kill majority, revive, verify recovery coordinator elected and all in-flight transactions resolved (committed or aborted). | `recovery_coordinator_activation`, `recovery_coordinator_resolves_inflight` |
| C6.5 | Clock-skew large: PreAccept rejection prevents corruption | clock-skew-large (±5s) | With large clock skew, Accord must reject or reorder — must not silently commit a transaction with a timestamp in the past that violates ordering. | `clock_skew_large_preaccept_rejection`, `clock_skew_large_no_ordering_violation` |
| C6.6 | Pause-resume state machine convergence | pause-node | Pause a node (SIGSTOP) for 30s, resume. Verify Accord state machine converges: no phantom writes, no lost ACKs that were in-flight during pause. | `pause_resume_state_convergence` |
| C6.7 | Disk-fail: no phantom commits | disk-fail | dm-flakey drops writes. Verify no client sees a committed write that was not durably stored. | `disk_fail_no_phantom_commits` |
| C6.8 | Packet-reorder: no linearizability violations from out-of-order delivery | packet-reorder | 25% reorder with 5ms gap. Verify Accord's ReorderBuffer correctly resequences messages. Knossos must pass. | `packet_reorder_linearizability` |
| C6.9 | LWT all-or-nothing under all failure modes | all 16 nemeses | For each nemesis: a BATCH CAS that includes 3 rows either fully commits or fully aborts. Never partial. | `lwt_batch_atomicity_all_nemeses` |
| C6.10 | Accord metrics are accurate under failures | all nemeses | `txn_in_flight`, `recovery_in_progress`, `fast_path_ratio` Prometheus gauges accurately reflect cluster state during and after each nemesis. | `accord_metrics_accurate_under_failures` |

**Refactoring required:**
- `ferrosa-cluster/src/pair/coordinator.rs`: Batch forwarding must be atomic (fix P1 hazard: per-mutation forwarding breaks batch atomicity)
- `ferrosa-storage/src/engine.rs:887-920`: Add idempotency token to commit-log replay to prevent duplicate application
- `ferrosa-cluster/src/raft/handlers.rs:196-265`: Validate digest mismatch re-fetch uses causal order, not just max-timestamp comparison

---

## Sprint C7: Compaction S3 Integration

**Duration:** 1 week
**Gate:** Write → flush → compact → S3 upload cycle completes without orphaned files. Cassandra 5.1 reader reads compacted SSTables from S3. Compaction metrics reflect correct state.

The compaction merge logic (`execute_task`), the result-polling swap (`poll_compactions`), and the background timer are all implemented. The missing piece is the S3 side: after `poll_compactions` swaps in the output SSTable, it never enqueues an S3 upload, updates the manifest, or deletes the superseded input SSTables from S3.

| # | Task | Size | Root Cause / Gap | Success Criteria | Tests |
|---|------|------|-----------------|-----------------|-------|
| C7.1 | Enqueue `UploadTask::SSTable` for compaction output in `poll_compactions` | S | `poll_compactions` opens and swaps the output SSTable locally but never submits an upload task to `upload_manager` | After `poll_compactions` succeeds, compacted SSTable components appear in S3 bucket. `upload_manager` metrics show task submitted. | `compaction_output_uploaded_to_s3` |
| C7.2 | Update manifest after compaction upload confirmed | M | Manifest is only updated on flush path (`sync_to_s3`). After compaction swap, manifest still lists input SSTables, not the output. | After upload confirmed: manifest removes input entries, adds output entry. CAS retry handles concurrent flushes during compaction. | `manifest_updated_after_compaction`, `manifest_compaction_concurrent_flush` |
| C7.3 | Delete input SSTable files from S3 after manifest update (1-hour grace period) | M | Input SSTables never deleted from S3 after compaction — unbounded S3 growth | Input SSTable files enqueued for deletion 1 hour after manifest update confirms output. Deletion task is idempotent (404 is not an error). | `compaction_inputs_deleted_from_s3_after_grace` |
| C7.4 | Delete input SSTable files from local disk after S3 deletion confirmed | S | Local disk growth: compacted inputs not cleaned up from `data_dir` after compaction swap | Input directories removed from local disk after S3 deletion and local cache eviction. `local_cache.evict()` called for each input. | `compaction_inputs_evicted_locally` |
| C7.5 | Compaction S3 round-trip: Cassandra 5.1 reads compacted SSTable from S3 | M | C5 validates flush output; compaction output must also pass Cassandra reader (separate code path through `execute_task`) | Cassandra reads compacted SSTable (2 input SSTables merged, written via `SSTableWriter`). All cell types preserved. Token order correct. | `cassandra_reads_compacted_sstable_from_s3` |
| C7.6 | Compaction observability: Prometheus metrics for S3 upload and deletion | S | No metrics for compaction S3 operations — cannot distinguish compaction from flush in dashboards | `ferrosa_compaction_s3_uploads_total`, `ferrosa_compaction_s3_deletes_total`, `ferrosa_compaction_input_bytes_reclaimed` counters present and accurate. | `compaction_s3_metrics_accurate` |
| C7.7 | Compaction end-to-end test: write 4 SSTables → compact → verify S3 → verify manifest → verify Cassandra reads | M | No integration test covers the full flush+compact+S3+manifest+Cassandra-reader pipeline | Single test exercises: 4 flush cycles (creates 4 SSTables), STCS triggers compact (merges to 1), upload confirms, manifest updated, old files deleted, Cassandra reader reads from S3. | `compaction_end_to_end_pipeline` |

**Refactoring required:**
- `ferrosa-storage/src/engine.rs`: Add `upload_manager` call in `poll_compactions` after successful swap
- `ferrosa-storage/src/engine.rs`: Add manifest update in `poll_compactions` (analogous to flush path)
- `ferrosa-storage/src/upload/manager.rs`: Add `UploadTask::DeleteSSTable` variant with grace-period delay

---

## Sprint C8: Full CQL Driver Compatibility

**Duration:** 1–2 weeks
**Gate:** All six CQL drivers execute a full compatibility smoke test against a live 3-node cluster without errors. `ferrosa-jepsen run --tier standard` passes with all 6 drivers. No driver-specific linearizability failures.

| # | Task | Size | Description | Success Criteria | Tests |
|---|------|------|------------|-----------------|-------|
| C8.1 | Python driver (`cassandra-driver`) compatibility smoke | S | Run `tests/drivers/python/test_cassandra_cql_examples.py` against live 3-node cluster (not standalone). Fix any cluster-mode regressions. | All Python smoke tests pass. 81.8%+ of CQL examples execute without error. | `python_driver_cluster_smoke` |
| C8.2 | Go driver (`gocql`) compatibility smoke | S | Run gocql smoke suite against 3-node cluster. Fix any cluster-mode regressions (peer topology, system tables, token-aware routing). | gocql connects, authenticates, executes DML, handles pagination. | `go_driver_cluster_smoke` |
| C8.3 | Java driver compatibility smoke | S | Run java-driver smoke suite (DML, prepared statements, batch, LWT) against 3-node cluster. | Java driver executes all statement types without error. | `java_driver_cluster_smoke` |
| C8.4 | Node.js driver (datastax) compatibility smoke | S | Run Node.js workload generator against 3-node cluster. | Node driver executes register and bank workloads end-to-end. | `node_driver_cluster_smoke` |
| C8.5 | C# driver compatibility smoke | S | Run C# driver workload against 3-node cluster. | C# driver connects, executes DML, handles LWT responses. | `csharp_driver_cluster_smoke` |
| C8.6 | Token-aware routing: all drivers route writes to correct coordinator | M | Each driver should send writes to the token-owner node, not always to node 1. Verify via `system.peers` and token ring metadata accuracy. | Each driver demonstrates token-aware routing in logs. No "wrong node" errors. | `token_aware_routing_all_drivers` |
| C8.7 | Prepared statement cache invalidation on schema change | M | All drivers cache prepared statements. After `ALTER TABLE`, cached statements must be invalidated and re-prepared correctly. | `ALTER TABLE ADD COLUMN` followed by prepared INSERT with new column succeeds for all 6 drivers. No stale metadata errors. | `prepared_stmt_cache_invalidation_all_drivers` |
| C8.8 | Fix any new CQL bugs surfaced by C8.1–C8.5 | L | Each driver test run is expected to surface 1–3 new protocol edge cases not covered by BUG-021–026 | All surfaced bugs fixed and covered by regression tests. CI gate green for all 6 drivers. | Per-bug regression tests |
| C8.9 | All-drivers Jepsen standard tier | L | Run `ferrosa-jepsen run --tier standard` with all 6 drivers simultaneously (not just Rust). Each driver runs workloads concurrently, all hitting the same cluster. | Zero anomalies across all 6 drivers. No driver-specific linearizability failures. Cross-driver invariants hold. | `jepsen_standard_tier_all_drivers` |

---

## Compiled Task Order and Dependencies

```
C1 (Bug fixes) ──────────────────────────────────────────────────┐
                                                                  ↓
C2 (P0 storage) ───────────────────────────────────────────────→ C4 (Jepsen T1+T2)
                                                                  ↑
C3 (Jepsen infra) ────────────────────────────────────────────────┘

C5 (SSTable compat) ─── parallel with C4, no dependency
                    ──→ feeds C7 (need compat tests before validating compaction output)

C6 (Accord under failures) ─── requires C4 (needs working Jepsen + C2 fixes)

C7 (Compaction S3) ─── requires C2 (S3 upload confirmation), C5 (Cassandra reader test)

C8 (CQL driver compat) ─── requires C3 (Jepsen infra), C4 (cluster working)
                        ─── parallel with C6, C7
```

C1 and C2 are strict prerequisites for C4. C3 can begin in parallel with C2. C5 is independent and can be parallelized with C4. C6 requires C4 as a baseline. C7 requires C2 and C5. C8 can begin once C3 and C4 are passing and runs in parallel with C6 and C7.

---

## Test Count by Sprint

| Sprint | New Tests | Cumulative | Gate |
|--------|-----------|-----------|------|
| C1 | ~14 | 14 | BUG-021–026 closed |
| C2 | ~8 | 22 | P0 hazards closed |
| C3 | ~14 | 36 | Smoke tier passes |
| C4 | ~40 | 76 | Standard tier passes (T1+T2) |
| C5 | ~16 | 92 | Cassandra reader CI gate green |
| C6 | ~20 | 112 | Accord correctness assertions pass all nemeses |
| C7 | ~10 | 122 | Compaction S3 round-trip + Cassandra reader |
| C8 | ~15 | 137 | All 6 drivers pass cluster smoke + Jepsen standard tier |

**Total: ~137 new tests across 8 sprints (~12–14 weeks)**

---

## Success Definition

This batch of work is complete when:

1. `ferrosa-jepsen run --tier standard` reports zero anomalies on a healthy 3-node and 5-node single-DC cluster with **all 6 drivers simultaneously**
2. `tests/sstable-compat/` CI gate passes: Cassandra 5.1 reads all ferrosa-written SSTable cell types — from both flush output and compaction output — fetched from S3
3. All C6 Accord correctness assertions pass across all 16 nemeses
4. The full flush → compact → S3 upload → manifest update → input deletion lifecycle completes without orphaned files or data loss
5. BUG-021 through BUG-026 are closed with regression tests
6. P0 hazards (C2.1–C2.3) are closed with durability tests
7. All six drivers pass cluster smoke tests and token-aware routing verification

At this point ferrosa is a **fully correct single-datacenter distributed database**: multi-node clustering works, compaction drains S3 correctly, and every CQL driver can run production workloads. The project is then ready to proceed to T3 (3+3 dual-DC) topologies per the unified project plan, with high confidence the single-DC foundation holds.

---

## Related Specs

- [jepsen-e2e-test-plan.md](jepsen-e2e-test-plan.md) — Full Jepsen architecture, topology definitions, nemesis matrix
- [accord.md](accord.md) — Accord consensus protocol spec
- [fmea-accord.md](fmea-accord.md) — Accord FMEA (failure mode analysis)
- [sstable.md](sstable.md) — SSTable format spec
- [project-plan-unified.md](project-plan-unified.md) — Full 18-sprint unified plan (this plan is an insert before A5)
- [testing.md](testing.md) — Testing strategy overview
