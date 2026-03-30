# TDD Plan: C1 + C2 + C3 + C7 Correctness Sprint Closure

> Created: 2026-03-29
> Status: **COMPLETE** — all 40 tests verified on `main` at `3f384ac` (2026-03-29)
> Scope: Close BUG-021–026, C2 P0 hazards, C3 Jepsen infrastructure, C7 compaction S3 tests.
> Runner: `cargo test`, `FERROSA_TEST_CONTAINERS=1 cargo test`
> TDD cycle: Red → Green → Refactor. One failing test at a time.
> PR: bkearns/ferrosa#84 (merged)

---

## Current State Summary

Before writing any test, note what the codebase exploration confirmed:

| Item | Code Status | Tests |
|------|-------------|-------|
| BUG-021 (bind values in QUERY) | **DONE** | 5 regression tests |
| BUG-022 (schema lost on restart) | **DONE** | 2 tests |
| BUG-023 (phonetic index lost) | **DONE** | 1 regression test |
| BUG-024 (PREPARE after ALTER TABLE) | **DONE** (ArcSwap) | 2 regression tests |
| BUG-025 (map bind value as blob) | **DONE** | 2 tests |
| BUG-026 (collection read-back fails) | **DONE** | 4 tests |
| C2.1 (pair replication ACK) | **DONE** | 3 tests |
| C2.2 (S3 upload before manifest) | **DONE** | 1 test (FailOnPutStore) |
| C2.3 (manifest CAS fallback) | **DONE** | 2 tests |
| C7.1-C7.4 Compaction S3 pipeline | **DONE** | 5 tests |
| C7.5 Cassandra reads compacted SST | **DONE** | Container test |
| C7.6 Prometheus metrics | **DONE** | 1 test |
| C7.7 End-to-end pipeline | **DONE** | Container test |
| C3.1-C3.7 Jepsen infrastructure | **DONE** | 12 tests |

---

## Master Test List

Check off as each test goes from red → green.

### C1: Open Bug Fixes

- [x] `collection_map_flush_readback` (C1.6 / BUG-026)
- [x] `collection_set_flush_readback` (C1.6 / BUG-026)
- [x] `collection_list_flush_readback` (C1.6 / BUG-026)
- [x] `collection_via_gocql_roundtrip` (C1.6 / BUG-026)
- [x] `map_bind_value_roundtrip` (C1.5 / BUG-025)
- [x] `map_bind_value_cassandra_compat` (C1.5 / BUG-025)
- [x] `bind_values_select` (C1.1 / BUG-021)
- [x] `bind_values_insert` (C1.1 / BUG-021)
- [x] `bind_values_update` (C1.1 / BUG-021)
- [x] `bind_values_delete` (C1.1 / BUG-021)
- [x] `bind_values_ten_types` (C1.1 / BUG-021)
- [x] `schema_survives_restart` (C1.2 / BUG-022)
- [x] `schema_survives_binary_upgrade` (C1.2 / BUG-022)
- [x] `phonetic_index_survives_restore` (C1.3 / BUG-023)
- [x] `prepare_after_alter_table_add_column` (C1.4 / BUG-024)
- [x] `prepare_after_alter_table_drop_column` (C1.4 / BUG-024)

### C2: P0 Storage Hazards

- [x] `manifest_cas_required_at_startup` (C2.3)
- [x] `manifest_concurrent_flush_preserves_all_entries` (C2.3)
- [x] `pair_write_confirmed_after_secondary_ack` (C2.1)
- [x] `pair_write_survives_primary_crash` (C2.1)
- [x] `pair_replication_timeout_returns_error` (C2.1)

### C7: Compaction S3 Integration (non-container tests)

- [x] `compaction_output_uploaded_to_s3` (C7.1)
- [x] `manifest_updated_after_compaction` (C7.2)
- [x] `manifest_compaction_concurrent_flush` (C7.2)
- [x] `compaction_inputs_enqueued_for_s3_deletion` (C7.3)
- [x] `compaction_inputs_evicted_locally` (C7.4)
- [x] `compaction_s3_metrics_accurate` (C7.6)

### C3: Jepsen Infrastructure

- [x] `rust_driver_connects_to_cluster` (C3.1)
- [x] `rust_driver_register_history_roundtrip` (C3.1)
- [x] `orchestrator_docker_cluster_provision` (C3.2) — `FERROSA_TEST_CONTAINERS=1`
- [x] `orchestrator_cluster_teardown` (C3.2) — `FERROSA_TEST_CONTAINERS=1`
- [x] `bank_workload_executes` (C3.3) — `FERROSA_TEST_CONTAINERS=1`
- [x] `lwt_insert_if_not_exists_executes` (C3.3) — `FERROSA_TEST_CONTAINERS=1`
- [x] `lwt_cas_counter_executes` (C3.3) — `FERROSA_TEST_CONTAINERS=1`
- [x] `bank_invariant_total_balance` (C3.4)
- [x] `register_invariant_every_read_valid` (C3.4)
- [x] `nemesis_partition_halves_docker` (C3.5) — `FERROSA_TEST_CONTAINERS=1`
- [x] `nemesis_kill_minority_docker` (C3.5) — `FERROSA_TEST_CONTAINERS=1`
- [x] `nemesis_clock_skew_docker` (C3.6) — `FERROSA_TEST_CONTAINERS=1`
- [x] `smoke_tier_end_to_end` (C3.7) — `FERROSA_TEST_CONTAINERS=1`

---

## Batch Execution Order

```
Batch A (no deps, run in parallel):
  C1.6 (BUG-026) collection storage
  C1.5 (BUG-025) map bind values
  C2.3 manifest CAS removal

Batch B (depends on A):
  C1.1 (BUG-021) bind values regression
  C1.2 (BUG-022) schema persistence
  C1.3 (BUG-023) phonetic index restore
  C1.4 (BUG-024) PREPARE after ALTER TABLE
  C2.1 pair replication ACK
  C7 non-container tests

Batch C (depends on B — needs live cluster):
  C3.1 Rust CQL driver
  C3.2 Docker provisioning
  C3.3 Workloads

Batch D (depends on C):
  C3.4 Invariant checkers
  C3.5/C3.6 Nemeses

Batch E (depends on D):
  C3.7 Smoke tier end-to-end
```

---

## Batch A, Task 1: BUG-026 — Collection Storage Read-Back

**Priority: Start here.** BUG-025 and BUG-026 share a root cause
(`raw_bytes_to_term` loses wire format). Fix BUG-025 first, then these
tests will drive BUG-026.

### Test 1 — `collection_map_flush_readback`

**File:** `ferrosa-storage/src/engine.rs` (test module)

```
Given: an engine with a table (pk text PRIMARY KEY, m map<text, int>)
When:  INSERT INTO t (pk, m) VALUES ('k1', {'a': 1, 'b': 2}), then flush
Then:  read 'k1' from SSTable → m == {'a': 1, 'b': 2}
```

```rust
#[test]
fn collection_map_flush_readback() {
    let dir = tempfile::tempdir().unwrap();
    let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
    let schema = TableSchema {
        keyspace: "ks".into(), table: "t".into(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".into(),
        clustering_columns: vec![],
        static_columns: vec![],
        regular_columns: vec![ColumnDefinition {
            name: "m".into(),
            type_name: "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.\
                        db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)".into(),
        }],
    };
    engine.register_table(schema).unwrap();
    let tid = TableId::new("ks", "t");

    // Encode a map<text,int> in Cassandra CQL binary format:
    // [2-byte element count][2-byte key len][key bytes][2-byte val len][val bytes]...
    let map_bytes = encode_cql_map(&[("a", 1i32), ("b", 2i32)]);
    let key = make_key("k1");
    let row = Row {
        clustering: vec![],
        cells: vec![(0, CellValue::live(map_bytes, 1000))],
        deletion: DeletionTime::LIVE,
        primary_key_liveness: LivenessInfo::with_timestamp(1000),
    };
    engine.write(&tid, &key, row, 1000).unwrap();
    engine.flush(&tid).unwrap();

    let result = engine.read(&tid, &key).unwrap().expect("row not found");
    let cell = &result.cells[0].1;
    let decoded = decode_cql_map(cell.value.as_ref().unwrap());
    assert_eq!(decoded.get("a"), Some(&1i32));
    assert_eq!(decoded.get("b"), Some(&2i32));
}
```

**Green path (if test fails):**
- Root cause is in `ferrosa-cql/src/connection.rs` — `raw_bytes_to_term()` converts
  CQL wire-format collection bytes into `Term::MapLiteral`, which `term_to_cql_value()`
  then serializes differently from the original wire bytes.
- Fix: in `raw_bytes_to_term()`, when the column type is `MapType`/`ListType`/`SetType`,
  pass the raw bytes through as `Term::Literal(raw_bytes)` — do NOT parse into
  `Term::MapLiteral`. The SSTable format stores the Cassandra wire-format bytes verbatim.
- Alternatively, in `term_to_cql_value()` for `Term::MapLiteral`, re-serialize back to
  Cassandra wire format (not Rust struct format).
- File: `ferrosa-cql/src/connection.rs`, function `raw_bytes_to_term()`.

**Refactor:** Extract `collection_wire_bytes_roundtrip` helper for set and list variants.

---

### Test 2 — `collection_set_flush_readback`

```
Given: table (pk text PRIMARY KEY, s set<text>)
When:  INSERT (pk='k1', s={'x','y','z'}), flush
Then:  read 'k1' → s contains exactly {"x","y","z"}
```

**Green path:** Same fix as above — collection bytes must be stored verbatim, not parsed.

---

### Test 3 — `collection_list_flush_readback`

```
Given: table (pk text PRIMARY KEY, l list<text>)
When:  INSERT (pk='k1', l=['p','q','r']), flush
Then:  read 'k1' → l == ["p","q","r"] (order preserved)
```

**Green path:** Same fix.

---

### Test 4 — `collection_via_gocql_roundtrip`

**File:** `ferrosa-storage/src/engine.rs` (test module), requires gocql subprocess
or a hand-rolled CQL frame encoding that matches gocql's wire output.

```
Given: ferrosa listening on port 9042 (embedded test server)
When:  gocql sends EXECUTE with a map<text,bigint> bind value
Then:  read back via SELECT → map matches inserted value
```

**Green path:** After fixing `raw_bytes_to_term()`, this is the full integration check.

---

## Batch A, Task 2: BUG-025 — Map Bind Value Decoded as Blob

**File:** `ferrosa-cql/src/connection.rs` (test module)

### Test 5 — `map_bind_value_roundtrip`

```
Given: prepared INSERT with column type map<text,int>
When:  EXECUTE with bind value [Cassandra wire-format map bytes for {'key':42}]
Then:  `raw_bytes_to_term(map_type, bytes)` returns Term that when stored
       and read back yields {'key': 42} — NOT a blob error
```

```rust
#[test]
fn map_bind_value_roundtrip() {
    use ferrosa_cql::types::{CqlType, raw_bytes_to_term, term_to_cell_value};
    let map_type = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int));
    let wire_bytes = encode_cql_map(&[("key", 42i32)]);

    // Must NOT return Term::Literal(4 bytes) — that is the bug
    let term = raw_bytes_to_term(&map_type, &wire_bytes).expect("decode failed");

    // Round-trip back to cell bytes
    let cell_bytes = term_to_cell_value(&term).expect("encode failed");
    let decoded = decode_cql_map(&cell_bytes);
    assert_eq!(decoded.get("key"), Some(&42i32));
}
```

**Green path (if test fails):**
- In `ferrosa-cql/src/connection.rs`, find `raw_bytes_to_term()`.
- Add match arms for `CqlType::Map`, `CqlType::List`, `CqlType::Set`.
- For `Map(k_type, v_type)`: parse CQL binary format: `u16 BE count`, then
  `u16 BE key_len + key_bytes + u16 BE val_len + val_bytes` per entry.
  Return `Term::MapLiteral(pairs)` (or simply `Term::CollectionBytes(wire_bytes.to_vec())`)
  — whichever the storage writer accepts correctly.
- The simpler fix: return `Term::Literal(wire_bytes.to_vec())` and ensure the storage
  path treats map/list/set column types as opaque variable-length byte arrays.
- File: `ferrosa-cql/src/connection.rs`.

### Test 6 — `map_bind_value_cassandra_compat`

```
Given: map<text,bigint> wire bytes from gocql (4-byte blob "test")
When:  raw_bytes_to_term called with MapType annotation
Then:  does NOT return a blob error; returns a decodable map
```

**Green path:** Same fix. This test specifically guards against the "4 bytes = blob" regression.

---

## Batch A, Task 3: C2.3 — Manifest CAS Required at Startup

**File:** `ferrosa-storage/src/manifest.rs`, `ferrosa-storage/src/engine.rs`

### Test 7 — `manifest_cas_required_at_startup`

```
Given: an object store that returns Err on conditional PUT (CAS not supported)
When:  StorageEngine::new_with_upload_store() is called
Then:  returns Err with a clear message about CAS requirement
```

```rust
#[tokio::test]
async fn manifest_cas_required_at_startup() {
    // Use a mock/stub store that always returns PreconditionFailed for conditional PUT
    let store = NoCasStore::new(); // fails conditional puts
    let result = StorageEngine::new_with_upload_store(
        StorageEngineConfig::test_config(tempdir.path()),
        Arc::new(store),
        "prefix".into(),
        &rt,
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("CAS"),
        "error must mention CAS requirement: {err}"
    );
}
```

**Green path (if test fails):**
- In `ferrosa-storage/src/engine.rs`, during startup, call `Manifest::probe_cas_support(store)`.
- If probe returns false, return `Err(Error::InvalidConfig("object store must support CAS..."))`.
- Delete the `cas_supported=false` branch in `manifest.rs:147-155` (the unconditional PUT).
- Files: `ferrosa-storage/src/engine.rs` (startup), `ferrosa-storage/src/manifest.rs` (remove fallback).

### Test 8 — `manifest_concurrent_flush_preserves_all_entries`

```
Given: engine with S3 store, CAS supported
When:  two flushes fire concurrently (tokio::join!)
Then:  manifest contains BOTH new SSTable entries (no lost-update)
```

**Green path:** With CAS fallback removed, this is a correctness proof test.
If it fails, the CAS retry loop in `manifest.rs:save_with_retry` needs a backoff fix.

---

## Batch B, Task 1: BUG-021 — Bind Values in QUERY Frame

**Note:** Code exploration shows `handle_query` already calls `substitute_bound_values`.
These tests are regression guards to ensure the fix stays wired correctly.

**File:** `ferrosa-cql/src/connection.rs` (test module)

### Test 9 — `bind_values_select`

```
Given: table (pk text PRIMARY KEY, v int), row ('k1', 99) inserted
When:  QUERY "SELECT v FROM t WHERE pk = ?" with bind value 'k1'
Then:  response contains row with v=99 (not empty result set)
```

**Green path:** `handle_query` reads flags byte, parses `[value]*`, calls `substitute_bound_values`.
If test fails, the flags byte parse is skipped for certain consistency levels — check
the frame parse cursor position after reading consistency.

### Test 10 — `bind_values_insert`

```
Given: empty table (pk text PRIMARY KEY, v int)
When:  QUERY "INSERT INTO t (pk, v) VALUES (?, ?)" with binds ['k2', 77]
Then:  subsequent SELECT pk='k2' → v == 77
```

### Test 11 — `bind_values_update`

```
Given: table with row ('k3', 0)
When:  QUERY "UPDATE t SET v=? WHERE pk=?" with binds [55, 'k3']
Then:  SELECT pk='k3' → v == 55
```

### Test 12 — `bind_values_delete`

```
Given: table with row ('k4', 1)
When:  QUERY "DELETE FROM t WHERE pk=?" with bind ['k4']
Then:  SELECT pk='k4' → no row
```

### Test 13 — `bind_values_ten_types`

```
Given: table with one column of each of 10 CQL types:
       text, int, bigint, boolean, uuid, timestamp, float, double, blob, inet
When:  INSERT via QUERY with bind values for all 10 columns
Then:  SELECT reads back all 10 values unchanged
```

**Green path:** If any type fails, find the type conversion in `raw_bytes_to_term()`.

---

## Batch B, Task 2: BUG-022 — Schema Survives Restart

**File:** `ferrosa-storage/src/engine.rs` (test module)

### Test 14 — `schema_survives_restart`

```
Given: engine started, table 'test_ks.widgets' created, 3 rows written+flushed
When:  engine dropped (simulating shutdown), new engine created at same data_dir
Then:  new engine has 'test_ks.widgets' in registry, rows readable
```

```rust
#[tokio::test]
async fn schema_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
        engine.register_table(test_schema()).unwrap();
        engine.write(&table_id(), &make_key("a"), make_row(b"v1", 1), 1).unwrap();
        engine.flush(&table_id()).unwrap();
        // engine dropped here — simulates shutdown
    }
    let engine2 = StorageEngine::new(StorageEngineConfig::test_config(dir.path())).unwrap();
    // Schema must be visible without calling register_table again
    assert!(
        engine2.table_schema(&table_id()).is_some(),
        "schema lost after restart"
    );
    let row = engine2.read(&table_id(), &make_key("a")).unwrap();
    assert!(row.is_some(), "data lost after restart");
}
```

**Green path (if test fails):**
- `StorageEngine::new()` must call `load_local_schema(data_dir)` on startup.
- `StorageEngine::flush()` must call `persist_schema_locally(data_dir)` after each flush.
- File: `ferrosa-storage/src/engine.rs`.

### Test 15 — `schema_survives_binary_upgrade`

```
Given: engine started with SSTable files present in data_dir AND schema.json present
When:  engine started (simulating binary upgrade — data_dir has existing files)
Then:  schema loaded from local schema.json regardless of SSTable presence
```

**Green path:** The `local_empty` check must be removed from the S3 bootstrap gate.
Schema loading from local file is unconditional on startup.

---

## Batch B, Task 3: BUG-023 — Phonetic Index Survives Restore

**File:** `ferrosa-schema/src/registry.rs` (test module)

### Test 16 — `phonetic_index_survives_restore`

```
Given: schema registry with a phonetic index on table 't.name'
When:  snapshot taken → registry cleared → snapshot applied via apply_snapshot()
Then:  phonetic index on 't.name' is present in restored registry
       SOUNDS LIKE queries return same results as before restore
```

**Green path (if test fails):**
- `apply_snapshot()` in `ferrosa-schema/src/registry.rs` iterates `snapshot.keyspaces`,
  `snapshot.tables`, `snapshot.roles`, `snapshot.grants` but NOT `snapshot.indexes`.
- Add a loop over `snapshot.indexes` using `entry().or_insert_with()` pattern matching
  the existing loops.
- Also add loops for `snapshot.types`, `snapshot.functions`, `snapshot.aggregates`.
- File: `ferrosa-schema/src/registry.rs`, function `apply_snapshot()`.

---

## Batch B, Task 4: BUG-024 — PREPARE After ALTER TABLE

**File:** `ferrosa-cql/src/connection.rs` (test module)

### Test 17 — `prepare_after_alter_table_add_column`

```
Given: table created with 3 columns (pk text, a int, b text)
When:  ALTER TABLE ADD c boolean; PREPARE "INSERT INTO t (pk,a,b,c) VALUES (?,?,?,?)"
Then:  PREPARE response result_metadata.columns.len() == 4
       EXECUTE with 4 bind values succeeds (no "expected N got M" error)
```

```rust
#[tokio::test]
async fn prepare_after_alter_table_add_column() {
    let server = spawn_test_server().await;
    let client = connect(&server).await;

    client.query("CREATE TABLE t (pk text PRIMARY KEY, a int, b text)").await.unwrap();
    client.query("ALTER TABLE t ADD c boolean").await.unwrap();

    let prepared = client.prepare("INSERT INTO t (pk,a,b,c) VALUES (?,?,?,?)").await.unwrap();
    // Must reflect 4 bind columns, not 3
    assert_eq!(prepared.bound_columns.len(), 4, "PREPARE must see post-ALTER schema");

    client.execute(&prepared, ("k1", 1i32, "hello", true)).await
        .expect("EXECUTE with 4 bind values must succeed");
}
```

**Green path (if test fails):**
- In `handle_prepare()`, the schema lookup resolves columns at PREPARE time from the
  registry snapshot. If `ALTER TABLE ADD` doesn't flush the snapshot or the lookup
  is cached stale, the PREPARE response is wrong.
- Fix: `handle_prepare()` must fetch from the live registry (not a snapshot taken at
  connection open). Check `state.schema_registry.current_snapshot()` vs a cached clone.
- File: `ferrosa-cql/src/connection.rs`, function `handle_prepare()`.

### Test 18 — `prepare_after_alter_table_drop_column`

```
Given: table with 5 columns; ALTER TABLE DROP removes column 'c'
When:  PREPARE "SELECT pk, a, b FROM t"
Then:  result_metadata does not include 'c'; column count is 3
```

---

## Batch B, Task 5: C2.1 — Pair Replication ACK

**File:** `ferrosa-cluster/src/pair/coordinator.rs`

### Test 19 — `pair_write_confirmed_after_secondary_ack`

```
Given: primary + secondary pair, secondary is healthy
When:  coordinate_write() called on primary
Then:  returns Ok ONLY AFTER secondary sends PairWriteAck
       (verified by delaying secondary ACK and asserting primary blocks)
```

```rust
#[tokio::test]
async fn pair_write_confirmed_after_secondary_ack() {
    let (primary, secondary, ack_trigger) = make_pair_with_delayed_ack(Duration::from_millis(50));

    let start = Instant::now();
    primary.coordinate_write(&test_mutation()).await.unwrap();
    let elapsed = start.elapsed();

    // Primary must have waited at least 50ms for secondary ACK
    assert!(elapsed >= Duration::from_millis(40), "primary did not wait for secondary ACK");
}
```

**Green path (if test fails):**
- In `coordinate_write()`, change the `PairRole::Primary` arm:
  ```rust
  PairRole::Primary => {
      self.apply_locally(mutation)?;
      self.replicate_to_peer(mutation).await
          .map_err(|e| ClusterError::ReplicationFailed(e.to_string()))?;
      Ok(())
  }
  ```
- Remove the `if let Err(e) = ... { tracing::warn!(...) }` fire-and-forget pattern.
- Add `ReplicationTimeout` error variant to `ClusterError` for timeout case.
- File: `ferrosa-cluster/src/pair/coordinator.rs:55-66`.

### Test 20 — `pair_write_survives_primary_crash`

```
Given: write acknowledged to client (secondary has it, primary crashes)
When:  primary restarts
Then:  data readable from secondary; no lost write
```

### Test 21 — `pair_replication_timeout_returns_error`

```
Given: secondary is unreachable (timeout after 5s)
When:  coordinate_write() called on primary
Then:  returns Err (not Ok) — client sees write failure, not silent loss
```

**Green path:** With fire-and-forget removed, this test is automatically satisfied by Test 19's fix.

---

## Batch B, Task 6: C7 Non-Container Tests

These tests verify the compaction S3 integration that is already implemented in
`poll_compactions`. They run without containers (using `InMemory` object store).

**File:** `ferrosa-storage/src/engine.rs` (test module)

### Test 22 — `compaction_output_uploaded_to_s3`

```
Given: engine with InMemory store, 4 SSTables flushed (triggering STCS compaction)
When:  poll_compactions() awaited
Then:  compacted SSTable files appear in InMemory store under correct S3 key prefix
       compaction_metrics.s3_uploads_total == 1
```

```rust
#[tokio::test]
async fn compaction_output_uploaded_to_s3() {
    let (engine, store, tid) = make_engine_with_4_sstables().await;
    engine.poll_compactions().await;

    let uploads = engine.compaction_metrics
        .s3_uploads_total.load(Ordering::Relaxed);
    assert_eq!(uploads, 1, "exactly one compacted SSTable must be uploaded");

    // Verify the S3 object exists
    let objects: Vec<_> = store.list(None).collect::<Vec<_>>().await;
    assert!(
        objects.iter().any(|o| o.as_ref().unwrap().location.to_string().contains("Data.db")),
        "Data.db must be present in S3"
    );
}
```

**Green path:** Implementation already exists in `poll_compactions`. If test fails, check
that `upload_manager` is `Some` in the test setup — use `new_with_upload_store`.

### Test 23 — `manifest_updated_after_compaction`

```
Given: engine with 4 SSTables, S3 store
When:  poll_compactions() completes
Then:  manifest loaded from S3 has exactly 1 entry (the output), not 4 (the inputs)
       manifest does NOT contain any input SSTable IDs
```

**Green path:** Already implemented in `poll_compactions` steps 4-5. If test fails, check
that `save_with_retry` is called with correct `s3_cas_supported` flag.

### Test 24 — `manifest_compaction_concurrent_flush`

```
Given: ongoing compaction (not yet complete) + a new flush fires concurrently
When:  both manifest updates race (CAS retry loop must handle version conflict)
Then:  final manifest contains both the compaction output AND the new flush SSTable
       (no entry is silently dropped)
```

**Green path:** CAS retry loop in `manifest.rs:save_with_retry` must retry on
`PreconditionFailed`. If the loop exits after max retries, return `Err` (not silently discard).

### Test 25 — `compaction_inputs_enqueued_for_s3_deletion`

```
Given: 4-SSTable compaction completes
When:  poll_compactions() returns
Then:  compaction_metrics.s3_deletes_total == 4 (one per input SSTable)
       (grace period is 1 hour — only enqueue, not await actual deletion in this test)
```

### Test 26 — `compaction_inputs_evicted_locally`

```
Given: 4-SSTable compaction completes
When:  poll_compactions() returns
Then:  local disk files for the 4 input SSTables no longer exist
       data_dir does NOT contain {gen}-Data.db for any input generation
```

### Test 27 — `compaction_s3_metrics_accurate`

```
Given: compaction of 4 SSTables (input_bytes_total = sum of component sizes)
When:  poll_compactions() completes
Then:  compaction_metrics.s3_uploads_total == 1
       compaction_metrics.s3_deletes_total == 4
       compaction_metrics.bytes_reclaimed > 0
```

---

## Batch C: C3.1 — Rust CQL Driver

**File:** `ferrosa-jepsen/src/driver/rust_driver.rs`

### Test 28 — `rust_driver_connects_to_cluster`

**Requires:** `FERROSA_TEST_CONTAINERS=1`, running 3-node ferrosa cluster

```
Given: 3-node ferrosa cluster running (Docker compose)
When:  RustDriver::run() called with contact_points = ["localhost:19042"]
Then:  CQL connection established, "SELECT now() FROM system.local" succeeds
       No panic, no hardcoded placeholder history returned
```

**Green path (TODO in rust_driver.rs):**
- Replace stub with `scylla` or `cdrs-tokio` crate session creation.
- Connect to `config.contact_points`, create keyspace/table for workload.
- Execute register workload: loop writes + reads, record to `HistoryRecorder`.
- File: `ferrosa-jepsen/src/driver/rust_driver.rs:27`.

### Test 29 — `rust_driver_register_history_roundtrip`

```
Given: driver connected, register workload runs for 5 seconds
When:  run() returns
Then:  output JSONL file exists with >= 10 operations
       each Op::Write has a corresponding Op::Read that returns its value
       no raw placeholder "test" keys appear in history
```

---

## Batch C: C3.2 — Docker Cluster Provisioning

**File:** `ferrosa-jepsen/src/orchestrator.rs`

### Test 30 — `orchestrator_docker_cluster_provision`

**Requires:** `FERROSA_TEST_CONTAINERS=1`

```
Given: no existing ferrosa cluster containers
When:  Orchestrator::provision_cluster(topology=T1_3_node) called
Then:  3 ferrosa containers running on ports 19042, 19043, 19044
       CQL SELECT from each node returns successfully within 30s
       "ferrosa-jepsen_node1_1" etc. container names present
```

**Green path:**
- Replace `tracing::info!("Provisioning cluster")` placeholder.
- Use `std::process::Command` to run `docker compose up -d` against a new
  `tests/docker/jepsen-cluster.yml` Compose file.
- Poll each node for CQL readiness (up to 30s) before returning.
- File: `ferrosa-jepsen/src/orchestrator.rs:57`.
- New file: `tests/docker/jepsen-cluster.yml` (3-node ferrosa + MinIO).

### Test 31 — `orchestrator_cluster_teardown`

```
Given: cluster provisioned by Test 30
When:  Orchestrator::teardown_cluster() called
Then:  all ferrosa containers stopped and removed
       ports 19042-19044 no longer listening
```

---

## Batch C: C3.3 — Bank and LWT Workloads

**File:** `ferrosa-jepsen/src/workload/bank.rs`, `ferrosa-jepsen/src/workload/lwt.rs`

### Test 32 — `bank_workload_executes`

**Requires:** `FERROSA_TEST_CONTAINERS=1`

```
Given: 3-node cluster, bank schema created (accounts table)
When:  bank workload runs for 10 seconds (20 clients)
Then:  at least 100 transfer operations executed
       all operations recorded in history with invoke/complete pairs
       no panics or connection errors during run
```

**Green path:**
- Replace stub in `workload/bank.rs` `run()`.
- Create `accounts` table, seed 5 accounts with balance=1000.
- Transfer loop: SELECT both accounts, UPDATE balances atomically (LWT or BATCH).
- Record every invoke/complete to history.

### Test 33 — `lwt_insert_if_not_exists_executes`

```
Given: cluster running
When:  "INSERT INTO t (pk, v) VALUES ('k', 1) IF NOT EXISTS" executed twice
Then:  first returns [applied]=true, second returns [applied]=false
       history records both outcomes correctly
```

### Test 34 — `lwt_cas_counter_executes`

```
Given: row ('k', current=0) in cluster
When:  "UPDATE t SET v=1 WHERE pk='k' IF v=0" executed
Then:  returns [applied]=true, v is now 1
       subsequent "IF v=0" returns [applied]=false
```

---

## Batch D: C3.4 — Invariant Checkers

**File:** `ferrosa-jepsen/src/checker/bank.rs`, `ferrosa-jepsen/src/checker/register.rs`

### Test 35 — `bank_invariant_total_balance`

```
Given: bank workload history with 200 transfer operations
When:  BankChecker::check(history) called
Then:  total balance at every observable point == 5000 (5 accounts × 1000)
       returns Ok if invariant holds, Err(violation) with offending operations if not
```

**Green path:**
- `BankChecker` scans history for every successful READ operation.
- Sums all account balances seen in that READ.
- If sum != initial_balance_total at any point, returns violation.
- File: `ferrosa-jepsen/src/checker/bank.rs`.

### Test 36 — `register_invariant_every_read_valid`

```
Given: register workload history (writes: W(1), W(2), W(3); reads: R(?))
When:  RegisterChecker::check(history) called
Then:  every R value is one of {initial, 1, 2, 3} — no value appears that was never written
       linearization is verified (each read returns the last written value that precedes it
       in real time — or a concurrent write)
```

---

## Batch D: C3.5 and C3.6 — Nemeses

**File:** `ferrosa-jepsen/src/nemesis/docker.rs` (new file)

### Test 37 — `nemesis_partition_halves_docker`

**Requires:** `FERROSA_TEST_CONTAINERS=1`

```
Given: 3-node cluster running
When:  Nemesis::partition_halves() called (disconnects node1 from node2+node3)
Then:  node1 cannot reach node2/node3 (CQL from node1 to system.peers shows 2 nodes unreachable)
       node2 and node3 can still reach each other
       heal() restores full connectivity within 5s
```

**Green path:**
- Use `docker network disconnect ferrosa-jepsen_default ferrosa-jepsen_node1_1`
  to isolate node1.
- Use `docker network connect` to heal.
- File: new `ferrosa-jepsen/src/nemesis/docker.rs`.

### Test 38 — `nemesis_kill_minority_docker`

```
Given: 3-node cluster
When:  Nemesis::kill_minority() kills node3 (docker stop)
Then:  node1 and node2 still accept reads/writes
       after Nemesis::revive(), node3 restarts and rejoins within 30s
```

### Test 39 — `nemesis_clock_skew_docker`

```
Given: 3-node cluster with faketime preload or /etc/faketime.conf support
When:  Nemesis::clock_skew(+100ms) applied to node2
Then:  node2's system.currenttimemillis returns value ~100ms ahead of node1
       Accord still accepts writes (clock skew within Accord's tolerance)
```

---

## Batch E: C3.7 — Smoke Tier End-to-End

**File:** `ferrosa-jepsen/src/orchestrator.rs`

### Test 40 — `smoke_tier_end_to_end`

**Requires:** `FERROSA_TEST_CONTAINERS=1`

```
Given: smoke tier config (T1/3-node, register workload, 12 clients, 30s run, 4 nemeses)
When:  ferrosa-jepsen run --tier smoke (or Orchestrator::run with smoke config)
Then:  completes in < 10 minutes
       history JSONL file written
       Rust checker exits 0 (no linearizability violations on healthy cluster)
       report.json written with pass/fail per workload×nemesis combination
```

**Green path:**
- Wire all C3.1–C3.6 components into `orchestrator.rs:run_single_combination()`.
- Replace the placeholder history with: provision → run driver + nemesis concurrently → check history.
- File: `ferrosa-jepsen/src/orchestrator.rs:152-159`.

---

## Implementation Notes

### Red → Green rule
Write EXACTLY ONE test. Run `cargo test [test_name]`. It must fail (red).
Then write the minimum code to make it pass (green). Then refactor.
Do not write code for tests that don't exist yet.

### Starting point
The optimal first test is **Test 5 (`map_bind_value_roundtrip`)** — it is a pure unit
test in `ferrosa-cql`, requires no infrastructure, and its fix unblocks Tests 6, 1, 2, 3, 4.

Second: **Test 7 (`manifest_cas_required_at_startup`)** — small change (delete 8 lines from
`manifest.rs:147-155`), high safety impact, runs in < 1 second.

Third: **Test 19 (`pair_write_confirmed_after_secondary_ack`)** — changes one match arm in
`pair/coordinator.rs`.

### Dependency of C3 on C1 + C2
C3 container tests require a correct server. Run C3 only after C1 and C2 tests are green.
Order in CI: `cargo test` (C1 + C2 + C7 non-container) → `FERROSA_TEST_CONTAINERS=1 cargo test` (C7 container + C3).

### C7 container tests
`cassandra_reads_compacted_sstable_from_s3` and `compaction_end_to_end_pipeline` exist
and have their bugs fixed. Run them after C7 non-container tests are green:
```
FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-storage \
  cassandra_reads_compacted_sstable_from_s3 \
  compaction_end_to_end_pipeline \
  -- --nocapture
```

### Files changed summary

| Sprint | File | Change |
|--------|------|--------|
| C1.5/C1.6 | `ferrosa-cql/src/connection.rs` | `raw_bytes_to_term()` collection handling |
| C1.2 | `ferrosa-storage/src/engine.rs` | `load_local_schema` on startup |
| C1.3 | `ferrosa-schema/src/registry.rs` | `apply_snapshot()` index/type loops |
| C1.4 | `ferrosa-cql/src/connection.rs` | `handle_prepare()` live schema lookup |
| C2.1 | `ferrosa-cluster/src/pair/coordinator.rs` | Remove fire-and-forget, await ACK |
| C2.3 | `ferrosa-storage/src/manifest.rs` | Delete `cas_supported=false` branch |
| C2.3 | `ferrosa-storage/src/engine.rs` | Add CAS probe at startup |
| C7 | `ferrosa-storage/src/engine.rs` | Tests only — implementation exists |
| C3.1 | `ferrosa-jepsen/src/driver/rust_driver.rs` | Replace stub with real CQL session |
| C3.2 | `ferrosa-jepsen/src/orchestrator.rs` | Real Docker provisioning |
| C3.2 | `tests/docker/jepsen-cluster.yml` | New 3-node compose file |
| C3.3 | `ferrosa-jepsen/src/workload/bank.rs` | Real CQL transfer workload |
| C3.3 | `ferrosa-jepsen/src/workload/lwt.rs` | Real LWT patterns |
| C3.4 | `ferrosa-jepsen/src/checker/` | Bank + register invariant checkers |
| C3.5/C3.6 | `ferrosa-jepsen/src/nemesis/docker.rs` | New Docker nemesis implementations |
| C3.7 | `ferrosa-jepsen/src/orchestrator.rs` | Wire all components into run_single_combination |
