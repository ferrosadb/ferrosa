# FMEA: Secondary Index Pipeline

> Last updated: 2026-03-18

## Failure Mode Analysis

| ID | Component | Failure Mode | Cause | Effect | S | O | D | RPN | Mitigation | Test Case |
|----|-----------|-------------|-------|--------|---|---|---|-----|------------|-----------|
| F1 | MemtableIndex | Index entry missing after write | ArcSwap race: reader loads old root before writer stores new root | Query misses recently written row | 7 | 2 | 5 | 70 | ArcSwap provides atomic load/store; path-copy creates new root before store. Reader either sees old or new — never partial. | Property test: concurrent writers + readers, all written keys eventually visible |
| F2 | MemtableIndex | OOM from large memtable index | Memtable grows to millions of entries before flush | Process crashes | 9 | 3 | 3 | 81 | Memtable index shares the same flush threshold as the memtable. When memtable flushes, index is serialized and dropped. | Stress test: write 1M rows, verify memory bounded by flush threshold |
| F3 | Sidecar Index | Sidecar file corrupt on disk | Partial write during flush, bit rot, S3 upload failure | Index lookup returns wrong RowPositions | 9 | 2 | 4 | 72 | CRC32 checksum in header (M1). Post-fetch validation (M2). Rebuild from SSTable on checksum failure. | Write sidecar, flip random byte, verify open() returns error |
| F4 | Sidecar Index | Sidecar missing for SSTable | Crash between SSTable flush and sidecar write | Queries using index miss data from this SSTable | 8 | 3 | 3 | 72 | Fallback: query full-scans SSTables without sidecar (M12). Startup rebuilds missing sidecars (M10). | Delete sidecar file, run indexed query, verify results include rows from that SSTable |
| F5 | Query Planner | Planner chooses index when full scan is faster | Low-selectivity index (e.g., boolean) matches WHERE clause | Query is slower than full scan | 3 | 5 | 7 | 105 | v1: accept suboptimal plans. v2: cost-based planner with cardinality estimation. | Benchmark: indexed boolean column query vs ALLOW FILTERING full scan |
| F6 | Query Planner | Planner misidentifies index match | Column name in WHERE matches index on different table/keyspace | Wrong index used, incorrect results | 9 | 1 | 2 | 18 | Planner resolves indexes by (keyspace, table, column) triple. Schema validation at CREATE INDEX time. | Test: two tables with same column name, different indexes, verify correct index used |
| F7 | IndexIntersection | Intersection returns empty set incorrectly | Bug in RowPosition equality (partition_key + clustering_key comparison) | Query returns no rows when rows exist | 9 | 2 | 3 | 54 | RowPosition derives PartialEq/Eq/Hash. Unit test intersection logic with known overlapping sets. | Test: insert rows matching both indexes, verify intersection returns them |
| F8 | Write Path | Write succeeds but index insert fails | Index column extraction fails (type mismatch, null value) | Index becomes stale — missing entries | 7 | 3 | 5 | 105 | Treat index insert failure as non-fatal warning (like Cassandra). Log and continue. Index will have gaps but write succeeds. | Test: write row with null indexed column, verify write succeeds and index handles gracefully |
| F9 | Compaction | Sidecar merge produces corrupt output | Bug in merge-sort: duplicate entries, missed tombstone | Index has phantom entries (rows that were deleted) | 8 | 2 | 4 | 64 | Post-merge validation: entry count matches. Tombstone-aware merge: skip entries whose row was deleted. | Test: insert rows, delete some, compact, verify index doesn't return deleted rows |
| F10 | Flush Path | Sidecar write blocks flush | Large index serialization takes too long | Write latency spike, memtable backpressure | 5 | 4 | 6 | 120 | Sidecar serialization happens after SSTable write completes. If too slow, can be done asynchronously (mark SSTable as pending in tracker, build sidecar in background). | Benchmark: flush with 5 indexes on 100K-row memtable, measure added latency |
| F11 | StorageEngine | read_by_index returns too many results | Low-cardinality indexed column, millions of matches | OOM (threat T2) | 9 | 4 | 3 | 108 | Cap results at 10,000 RowPositions (M4). Return error suggesting ALLOW FILTERING. | Test: index boolean column, write 100K rows, query, verify capped result |
| F12 | EXPLAIN | EXPLAIN returns stale plan | Schema changed between EXPLAIN and execution | Misleading plan output | 2 | 3 | 8 | 48 | EXPLAIN is advisory. Document that plans reflect schema at query time. | N/A (informational only) |

## Risk Priority Summary

| RPN Range | Count | Items |
|-----------|-------|-------|
| Critical (>= 200) | 0 | — |
| High (100-199) | 3 | F5 (105), F8 (105), F10 (120), F11 (108) |
| Medium (50-99) | 4 | F1 (70), F2 (81), F3 (72), F4 (72) |
| Low (< 50) | 5 | F6 (18), F7 (54), F9 (64), F12 (48) |

## Test Cases for RPN >= 50

| Test ID | FMEA Ref | Test Description | Expected Result |
|---------|----------|-----------------|-----------------|
| TC1 | F1 | Spawn 10 writer threads + 10 reader threads on same MemtableIndex; all written keys eventually visible to readers | No missing keys after all writers complete |
| TC2 | F2 | Write rows until memtable flush triggers; verify MemtableIndex is dropped after flush | Memory usage returns to baseline after flush |
| TC3 | F3 | Write sidecar file, corrupt 1 byte, attempt to open | `IndexError::Corrupt` returned, not silent wrong data |
| TC4 | F4 | Delete sidecar file, run indexed query | Results include rows from the SSTable that lost its sidecar |
| TC5 | F5 | Benchmark: `SELECT * FROM t WHERE bool_col = true` with index vs ALLOW FILTERING | Document performance characteristics |
| TC6 | F7 | Insert rows matching predicates A and B, run `WHERE a = x AND b = y` with IndexIntersection | Correct rows returned (not empty, not superset) |
| TC7 | F8 | INSERT with null value for indexed column | Write succeeds, index gracefully skips null |
| TC8 | F9 | Insert 1000 rows, delete 500, trigger compaction, query via index | Only 500 surviving rows returned |
| TC9 | F10 | Flush table with 5 secondary indexes, 100K rows, measure wall-clock time | Flush completes within 2x of non-indexed flush |
| TC10 | F11 | Index a boolean column, write 100K rows, query `WHERE bool_col = true` | Error returned with suggestion, not OOM |
