# Bug: DROP TABLE Does Not Delete Data

**Severity:** High
**Component:** ferrosa-storage (DDL)
**Branch:** fix/p0-compaction-ddl-readiness

## Issue

`DROP TABLE IF EXISTS agent_memory.co_occurs_with` followed by `CREATE TABLE agent_memory.co_occurs_with (...)` leaves the old data intact. `SELECT count(*)` returns the same count (7,382) before and after the DROP+CREATE cycle. All 3 nodes return the same stale data.

`TRUNCATE` also has no effect — count remains unchanged after truncate.

## Expected Behavior

DROP TABLE should delete all data in the table. TRUNCATE should delete all rows. After DROP+CREATE, the table should be empty.

## Reproduction

```python
session.execute('SELECT count(*) FROM agent_memory.co_occurs_with')  # 7382
session.execute('DROP TABLE IF EXISTS agent_memory.co_occurs_with')
session.execute('CREATE TABLE IF NOT EXISTS agent_memory.co_occurs_with (...)')
session.execute('SELECT count(*) FROM agent_memory.co_occurs_with')  # still 7382
```

Also tested TRUNCATE:
```python
session.execute('TRUNCATE agent_memory.co_occurs_with')
session.execute('SELECT count(*) FROM agent_memory.co_occurs_with')  # still 7382
```

## Impact

- Cannot clean up stale data
- Schema migrations that drop+recreate tables retain old data
- No way to remove unwanted edges from the knowledge graph via DDL

## Likely Cause

SSTable files on disk are not deleted when DROP TABLE is processed. The new table picks up the old SSTables because they have the same table name/ID.
