# Bug: DROP TABLE Does Not Delete Data

**Severity:** High
**Component:** ferrosa-storage (DDL)
**Branch:** docs/marketing-updates (commit 8ecad44)

## Issue

`DROP TABLE IF EXISTS agent_memory.co_occurs_with` followed by `CREATE TABLE agent_memory.co_occurs_with (...)` leaves the old data intact. `SELECT count(*)` returns the same count (7,382) before and after the DROP+CREATE cycle. All 3 nodes return the same stale data.

`TRUNCATE` also has no effect — count remains unchanged after truncate.

## Root Cause

`unregister_table()` only removed the `TableState` from the in-memory map but did NOT delete SSTable files from `data_dir/sstables/{keyspace}.{table}/`. When `register_table_inner()` ran for the re-created table, `load_existing_sstables_and_sidecars()` scanned the directory and loaded the old SSTables.

## Fix (commit 8ecad44)

- `unregister_table()`: now calls `std::fs::remove_dir_all()` on the table's SSTable directory (`data_dir/sstables/{keyspace}.{table}/`)
- `truncate()`: now deletes and re-creates the SSTable directory so data doesn't reappear on restart

## Verification

### Test: `drop_table_then_recreate_starts_empty`

```
write("old_data") → flush to SSTable → verify data exists
→ unregister_table() (DROP TABLE)
→ register_table() (CREATE TABLE)
→ read("old_data") returns None ✓
```

### Test: `truncate_deletes_flushed_data`

```
write("trunc_data") → flush to SSTable → verify data exists
→ truncate()
→ read("trunc_data") returns None ✓
```

### Test: `drop_one_table_does_not_affect_other_tables`

Reproduces the exact scenario from the follow-up bug report (bug-data-loss-after-drop-table):

```
register entity_store + co_occurs_with (same keyspace)
write+flush data to BOTH tables
DROP TABLE co_occurs_with
→ entity_store data STILL PRESENT ✓
→ entity_store SSTable directory STILL EXISTS ✓
→ co_occurs_with SSTable directory DELETED ✓
```

This proves `remove_dir_all` correctly targets only the dropped table's directory path (`data_dir/sstables/test_ks.co_occurs_with/`), not the parent or sibling directories.

### All 3 tests pass

```
cargo test -p ferrosa-storage --lib -- drop_table_then_recreate drop_one_table truncate_deletes
running 3 tests
test engine::tests::drop_table_then_recreate_starts_empty ... ok
test engine::tests::drop_one_table_does_not_affect_other_tables ... ok
test engine::tests::truncate_deletes_flushed_data ... ok
test result: ok. 3 passed; 0 failed
```
