# Test Specification: UDF/UDA Branch Integration with Accord Transactions

> Branch: `feature/udf-uda-query-time`
> Last updated: 2026-03-21
> Status: Specification (pre-implementation)

This document specifies tests that verify the `feature/udf-uda-query-time` branch changes integrate correctly with Accord consensus transactions. Each section corresponds to a specific merge point or feature addition and its interaction with Accord's commit, apply, and conflict-detection paths.

---

## 1. DeleteTarget Serialization (S4.7)

The UDF/UDA branch introduced `DeleteTarget` (`ferrosa-cql/src/ast.rs:213`) with two variants:

- `Column(String)` -- delete an entire column
- `MapElement { column: String, key: Term }` -- delete a single map element (`DELETE col['key'] FROM ...`)

Accord commit log entries that record applied transactions must serialize both variants faithfully. The `DeleteStatement.columns` field (`ast.rs:234`) is `Vec<DeleteTarget>`, so a single transaction can contain a mix of both variants.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_delete_target_column_roundtrip` | `DeleteTarget::Column` survives Accord commit log serialization and deserialization without data loss. | Create an `AccordApplied` commit log entry containing a DELETE with `DeleteTarget::Column("age")`. Serialize to the protocol log wire format. Deserialize back. Assert: the deserialized variant is `Column`, column name is `"age"`. |
| `accord_delete_target_map_element_roundtrip` | `DeleteTarget::MapElement` with a `Term::String` key roundtrips through the Accord commit log. The key term type and value are preserved, not flattened to a string. | Create an `AccordApplied` entry containing DELETE with `DeleteTarget::MapElement { column: "props", key: Term::String("color") }`. Serialize. Deserialize. Assert: variant is `MapElement`, column is `"props"`, key is `Term::String("color")` (not `Term::StringLiteral`). |
| `accord_delete_target_apply_column` | Accord's apply handler correctly removes an entire column from the memtable when processing a `Column` delete. The `MemIndex` entry for the old column value is also cleaned up, preventing stale index hits. | Apply handler receives a transaction that deletes column `"age"` from a row with `age=42`. Assert: after apply, the column is absent from the row in the memtable (`get` returns a row without `"age"`). A `MemIndex` query for `age=42` no longer returns this row. |
| `accord_delete_target_apply_map_element` | A `MapElement` delete only removes the targeted element, not the entire map column. Other map entries are preserved. | Apply handler receives a transaction that deletes map element `props['color']` from a row where `props = {'color': 'red', 'size': 'large'}`. Assert: after apply, `props` still exists with `{'size': 'large'}`. Only the `'color'` entry is removed. |
| `accord_delete_target_mixed_batch` | A single Accord transaction can contain both `Column` and `MapElement` deletes in the same batch. Both are serialized in one commit log entry and both are applied atomically. | Transaction batch contains `DeleteTarget::Column("email")` and `DeleteTarget::MapElement { column: "tags", key: Term::String("deprecated") }`. Serialize to commit log. Deserialize. Assert: both targets are present in the deserialized entry. Apply the transaction. Assert: `email` column is gone, `tags` map no longer contains `'deprecated'`, other `tags` entries are intact. |

**Source files:**

- `ferrosa-cql/src/ast.rs` -- `DeleteTarget` enum (line 213), `DeleteStatement` (line 231)
- `ferrosa-cql/src/parser.rs` -- `parse_delete_target()` (line 414), existing test coverage at line 2432

---

## 2. Token Function Range Routing (S4.8)

The UDF/UDA branch added `WhereClause.token_fn: bool` (`ferrosa-cql/src/ast.rs:140`) for token-range predicates like `WHERE token(id) > token(3)`. The planner (`ferrosa-cql/src/planner.rs:93`) and router (`ferrosa-cql/src/router.rs:1107, 3885`) filter these out of column-level WHERE evaluation. Accord routing must treat token-function predicates as range operations for conflict detection.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_token_fn_range_routing` | Token-function WHERE clauses are registered as range operations in the ConflictIndex, not point lookups. The token range boundaries are extracted from the `Term::FunctionCall("token", ...)` RHS values. | Parse query: `SELECT * FROM t WHERE token(id) > token(3) AND token(id) < token(100)`. Route through Accord's conflict detection. Assert: the query registers in the ConflictIndex's `range_ops` BTreeMap (not the `single_key` HashMap). The registered range spans `[token(3), token(100)]`. |
| `accord_token_fn_conflict_with_point_write` | Accord detects conflicts between a point write and an overlapping range read. A range query that spans a token must see any concurrent writes to tokens within that range as dependencies. | Accord txn T1 writes a key whose Murmur3 token hashes to 50. Range query T2 covers token range [0, 100]. Assert: ConflictIndex detects conflict between T1 and T2. T2's dependency set contains T1's transaction ID. |
| `accord_token_fn_no_conflict_outside_range` | Range operations that do not overlap a point write's token are not false-positive conflicts. Unnecessary conflicts would serialize unrelated transactions and degrade throughput. | T1 writes a key with token=150. Range query T2 covers [0, 100]. Assert: no conflict detected. T2's dependency set does NOT contain T1. |
| `accord_token_fn_full_scan` | A full table scan (no WHERE clause) is equivalent to a range over ALL tokens and conflicts with every in-flight transaction on the same table. This validates the worst-case path and explains why `ALLOW FILTERING` queries are expensive under Accord. | Parse query: `SELECT * FROM t` (no WHERE clause). Route through Accord's conflict detection with three in-flight write transactions T1, T2, T3 on table `t`. Assert: the scan is treated as a range spanning `[Token::MIN, Token::MAX]`. Its dependency set contains all three transactions. |

**Source files:**

- `ferrosa-cql/src/ast.rs` -- `WhereClause.token_fn` (line 140)
- `ferrosa-cql/src/parser.rs` -- token function parsing (line 1577-1602), test at line 2956
- `ferrosa-cql/src/planner.rs` -- token predicate filtering (line 93)
- `ferrosa-cql/src/router.rs` -- token predicate skip in WHERE eval (line 1107, 3885)

---

## 3. Late Data Debouncer Ordering (S2.6 -- FM15, RPN 196)

The UDF/UDA branch added `LateDataDebouncer` (`ferrosa-storage/src/timeseries/late_data.rs:22`) for RRD timeseries re-aggregation debouncing. Its current implementation uses `Instant::now()` for timestamp tracking (`late_data.rs:79`). Under Accord, ordering must use Accord-assigned timestamps -- not wall clock -- to ensure deterministic re-aggregation across replicas. Failure to do so is FMEA failure mode FM15 (replica divergence on late-data re-aggregation).

The `LateDataKey` struct (`late_data.rs:11`) contains `window_start_ts: i64`. This timestamp must be derived from the Accord transaction timestamp, not from `SystemTime::now()` at the receiving node.

| Test | What It Proves | How |
|------|---------------|-----|
| `debouncer_accord_timestamp_ordering` | Re-aggregation ordering is determined by the Accord transaction timestamp, not wall-clock arrival time. Two replicas that receive the same late data at different wall-clock times produce identical results. | Simulate two nodes receiving the same late data point. Node A processes it at `wall_clock=100`, node B at `wall_clock=200`. Both use the Accord timestamp of the originating write (`accord_ts=50`) for ordering. Assert: both nodes produce the same re-aggregation result. The `LateDataKey.window_start_ts` is derived from the Accord timestamp's window boundary, not from `Instant::now()`. |
| `debouncer_concurrent_late_data_and_txn` | When an Accord transaction writes a timeseries value concurrently with a debouncer-triggered re-aggregation for the same window, the debouncer either sees the transaction's write (linearizable) or dep-waits for it. No race where re-aggregation completes with stale data while the transaction is still in-flight. | Start an Accord transaction T1 that writes to timeseries window W. Concurrently trigger re-aggregation for window W via the debouncer. Assert: the re-aggregation either (a) includes T1's value in its computation, or (b) blocks until T1 completes and then re-aggregates. The ConflictIndex shows the re-aggregation read conflicts with T1. |
| `debouncer_deterministic_across_replicas` | Three replicas receiving late data in different arrival orders all produce identical aggregation results. This is the direct test of FMEA FM15 (replica divergence). | Three replicas R1, R2, R3. Late data points A(accord_ts=10), B(accord_ts=20), C(accord_ts=15) arrive at R1 in order [A, B, C], at R2 in order [C, A, B], at R3 in order [B, C, A]. All three re-aggregate using Accord timestamps for ordering. Assert: all three produce identical final aggregation state. If any diverge, FM15 has occurred. |
| `debouncer_window_start_ts_from_accord` | The `LateDataKey.window_start_ts` field uses the Accord-assigned transaction timestamp to compute the window boundary, not `SystemTime::now()`. This prevents clock-skew between nodes from placing the same event into different windows. | Write a timeseries value via Accord with `accord_ts = 1_500_000` (microseconds). The RRD window size is 1,000,000 microseconds. Assert: the resulting `LateDataKey` has `window_start_ts = 1_000_000` (floor of accord_ts to window boundary). Not `SystemTime::now()` floored to the window boundary. |

**Source files:**

- `ferrosa-storage/src/timeseries/late_data.rs` -- `LateDataDebouncer` (line 22), `LateDataKey` (line 11)
- `ferrosa-storage/src/timeseries/aggregator.rs` -- `SmallVec<[f64; 8]>` usage (line 145)
- `specs/fmea-rrd-timeseries.md` -- FM6 (debouncer unbounded growth, mitigated)

**FMEA cross-reference:** FM15 is not yet in `specs/fmea-rrd-timeseries.md`. These tests define the acceptance criteria for adding FM15 (replica divergence on late-data re-aggregation, estimated RPN 196 = S:7 x O:4 x D:7).

---

## 4. Row-Level Deletion LWW Idempotency (UDF/UDA Branch Merge Point)

The UDF/UDA branch changed `ferrosa-storage/src/memtable/sharded.rs` (line 194) to use LWW (last-write-wins) for row-level deletion:

```rust
if new_row.deletion.marked_for_delete_at > existing_row.deletion.marked_for_delete_at {
    existing_row.deletion = new_row.deletion;
}
```

This is inside `merge_row_into_partition()` (line 182). Accord replays (commit log recovery, replica catch-up) must be idempotent with this LWW logic -- applying the same mutation twice must produce the same state as applying it once.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_deletion_lww_idempotent` | Replaying the same DELETE from the Accord commit log produces no side effects. The LWW comparison `>` (not `>=`) means an equal timestamp does not re-apply the deletion. | Apply a DELETE with `deletion.marked_for_delete_at = 10`. Apply the exact same DELETE again (simulating commit log replay). Assert: no error, no panic, no double-delete. Row state after the second apply is identical to state after the first. The `marked_for_delete_at` field is still 10, `local_deletion_time` unchanged. |
| `accord_deletion_lww_ordering` | A sequence of INSERT, DELETE, INSERT with increasing timestamps follows LWW correctly. The final INSERT supersedes the intermediate DELETE because its write timestamp is higher. | Apply INSERT with `primary_key_liveness.timestamp = 10`. Apply DELETE with `deletion.marked_for_delete_at = 15`. Apply INSERT with `primary_key_liveness.timestamp = 20` and new cell values. Assert: the row exists in the memtable with the cells from the second INSERT. `deletion.marked_for_delete_at` remains 15 (preserved by LWW), but the row is live because `liveness.timestamp = 20 > deletion.marked_for_delete_at = 15`. |
| `accord_deletion_lww_replay_order_independent` | The same set of operations applied in different orders produces the same final state. This is the fundamental CRDT convergence property required by Accord. | Forward order: INSERT(ts=10), then DELETE(ts=15). Reverse order: DELETE(ts=15), then INSERT(ts=10). Assert: both orders produce the same final memtable state. In both cases, `deletion.marked_for_delete_at = 15` and `primary_key_liveness.timestamp = 10`, so the row is effectively deleted (deletion timestamp > liveness timestamp). |
| `accord_deletion_lww_concurrent_replicas` | Two replicas applying the same DELETE and INSERT in different network-reordered sequences converge to identical state. This is the multi-node version of the order-independence test. | Replica R1 applies: INSERT(ts=10), DELETE(ts=15), INSERT(ts=20). Replica R2 applies: DELETE(ts=15), INSERT(ts=20), INSERT(ts=10). Assert: both replicas' memtables contain a row with `deletion.marked_for_delete_at = 15`, `primary_key_liveness.timestamp = 20`, and the cells from INSERT(ts=20). The cell-level LWW in `merge_row_into_partition` ensures per-cell convergence as well. |

**Source files:**

- `ferrosa-storage/src/memtable/sharded.rs` -- `merge_row_into_partition()` (line 182), LWW deletion comparison (line 194)
- `ferrosa-sstable/src/types.rs` -- `DeletionTime` struct with `marked_for_delete_at: i64` and `local_deletion_time: u32`

---

## 5. Commit Log Oversized Entry Handling (S1 Merge Point)

The UDF/UDA branch changed the commit log (`ferrosa-storage/src/commitlog/mod.rs:198-203`) to return `Err(InvalidData)` instead of panicking when an entry exceeds `segment_size`. The original code would `panic!()` on oversized entries, which would crash the entire node. The new behavior is:

```rust
return Err(ferrosa_common::Error::InvalidData(format!(
    "commit log entry ({total_size} bytes) exceeds segment capacity; increase segment_size"
)));
```

Accord transactions can produce large results (e.g., a read-modify-write with a large blob). The apply handler must propagate this error gracefully, abort the transaction, and clean up ConflictIndex state.

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_oversized_entry_error` | An Accord transaction whose commit log entry exceeds `segment_size` returns a clean error, not a panic. The transaction is aborted and its ConflictIndex entry is cleaned up so it does not permanently block conflicting transactions. | Create an Accord transaction with a large payload (e.g., a 100MB blob result). Configure `segment_size = 512` (test config, `config.rs:181`). Attempt to write `AccordApplied` to the commit log. Assert: returns `Err(InvalidData)` with message containing "exceeds segment capacity". The transaction's entry in the ConflictIndex is removed. The client receives an error response (not a connection drop from panic). |
| `accord_oversized_entry_other_txns_unaffected` | A failed oversized entry does not affect other in-flight transactions on the same shard. The segment is not corrupted by a partial write. | Transaction T1 has an oversized entry (fails with `InvalidData`). Transaction T2 (normal size, ~118 bytes) is in-flight on the same segment. Assert: T2's append succeeds. T2's commit log entry is readable and valid. The segment's CAS allocator state is consistent (no gap from T1's failed allocation). |
| `accord_entry_size_within_segment` | A normal Accord transaction with typical metadata (deps stored in `SmallVec<[TxnId; 8]>` inline, ~1KB result) fits within the default segment size. This is the happy-path baseline. | Create an Accord transaction with 8 dependencies (fits in SmallVec inline storage) and a 1KB result payload. Use default `segment_size = 32MB` (`DEFAULT_SEGMENT_SIZE`, `config.rs:150`). Assert: append succeeds. Entry is readable on replay. All 8 dependency TxnIds are preserved. |

**Source files:**

- `ferrosa-storage/src/commitlog/mod.rs` -- oversized entry error (line 198-203), `Segment::new` (line 99)
- `ferrosa-storage/src/commitlog/config.rs` -- `DEFAULT_SEGMENT_SIZE = 32MB` (line 150), `segment_size` field (line 163), `test_config` with 4KB segments (line 181)
- `ferrosa-storage/src/commitlog/segment.rs` -- CAS allocation

---

## 6. PreparedStatement pk_indexes Consistency (UDF/UDA Branch)

The UDF/UDA branch added `pk_indexes` to the PREPARE response metadata (`ferrosa-cql/src/connection.rs:793-799`, `ferrosa-cql/src/result.rs:110-122`). These indexes tell CQL drivers which bind variables correspond to partition key columns, enabling token-aware routing. The `compute_pk_indexes()` function (`connection.rs:1115`) resolves PK column positions from the schema.

For Accord, `pk_indexes` consistency across replicas is critical: if two replicas return different `pk_indexes` for the same prepared statement, drivers may route the Accord coordinator request to the wrong node, forcing a slow-path fallback (coordinating from a non-replica).

| Test | What It Proves | How |
|------|---------------|-----|
| `accord_pk_indexes_consistent` | The same PREPARE statement sent to 3 different nodes returns identical `pk_indexes` metadata. Divergent metadata would cause drivers to compute different routing tokens, leading to unnecessary Accord slow-path fallbacks and increased latency. | PREPARE `INSERT INTO ks.t (id, name, value) VALUES (?, ?, ?)` on nodes N1, N2, N3. Table `ks.t` has partition key `(id)`. Assert: all 3 nodes return the same `pk_count = 1` and `pk_indexes = [0]` (bind position 0 is `id`). Verify using `encode_prepared_bind_metadata()` output byte-for-byte comparison. |
| `accord_pk_indexes_after_schema_change` | After an ALTER TABLE that changes the column layout, re-PREPAREing the same statement returns updated `pk_indexes`. Stale `pk_indexes` from a cached prepared statement could route Accord transactions to the wrong partition. | PREPARE `INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)` where PK is `(a)`. Assert: `pk_indexes = [0]`. Execute `ALTER TABLE ks.t ADD d int`. PREPARE the same statement again. Assert: `pk_indexes` are still `[0]` (adding a non-PK column does not change PK bind positions). Then PREPARE `INSERT INTO ks.t (a, b, c, d) VALUES (?, ?, ?, ?)`. Assert: `pk_indexes = [0]` (PK column `a` is still at bind position 0). Verify that the old prepared statement ID is invalidated (schema change bumps the result_metadata_id). |

**Source files:**

- `ferrosa-cql/src/connection.rs` -- `compute_pk_indexes()` (line 1115), PREPARE handler (line 790-830)
- `ferrosa-cql/src/result.rs` -- `encode_prepared()` (line 113), `encode_prepared_bind_metadata()` (line 176), tests at line 416, 517, 543

---

## Test Implementation Notes

### Crate locations for test files

| Section | Suggested test file | Crate |
|---------|-------------------|-------|
| S4.7 DeleteTarget | `ferrosa-cql/tests/accord_delete_target.rs` | ferrosa-cql |
| S4.8 Token Range | `ferrosa-cql/tests/accord_token_range.rs` | ferrosa-cql |
| S2.6 Late Data | `ferrosa-storage/tests/accord_late_data.rs` | ferrosa-storage |
| LWW Deletion | `ferrosa-storage/tests/accord_deletion_lww.rs` | ferrosa-storage |
| Oversized Entry | `ferrosa-storage/tests/accord_commitlog_oversize.rs` | ferrosa-storage |
| pk_indexes | `ferrosa-cql/tests/accord_pk_indexes.rs` | ferrosa-cql |

### Prerequisites

Several of these tests require Accord infrastructure that does not yet exist in the codebase:

1. **AccordApplied commit log entry type** -- The commit log currently records `Mutation` entries. Accord needs a distinct entry type that includes the transaction ID, dependency set, and applied result.
2. **ConflictIndex** -- A data structure (likely `BTreeMap` for ranges, `HashMap` for point keys) that tracks in-flight Accord transactions for conflict detection. Referenced in S4.8 tests.
3. **Accord transaction ID (`TxnId`)** -- A unique identifier for each Accord transaction, used in dependency tracking.

Tests in sections 1, 2, and 5 can be partially implemented using existing commit log and parser infrastructure. Tests in section 3 require the debouncer to accept an explicit timestamp parameter (replacing `Instant::now()` with an injected Accord timestamp). Tests in section 4 can be implemented today using the existing `merge_row_into_partition()` function.

### Priority order

1. **Section 4** (LWW idempotency) -- Can be tested now with existing code. Highest correctness risk.
2. **Section 5** (oversized entry) -- Can be tested now. Prevents node crashes.
3. **Section 1** (DeleteTarget serialization) -- Requires AccordApplied entry type. Medium risk.
4. **Section 6** (pk_indexes) -- Can be partially tested now. Low risk if schema propagation works.
5. **Section 2** (token range routing) -- Requires ConflictIndex. High complexity.
6. **Section 3** (debouncer ordering) -- Requires Accord timestamp injection. Architectural change needed.
