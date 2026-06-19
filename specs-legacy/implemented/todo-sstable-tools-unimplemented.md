# TODO: SSTable CLI Tools Not Implemented

**Severity:** Low
**Component:** ferrosa-sstable
**Files:** `ferrosa-sstable/src/bin/ferrosa-sstable-dump.rs:23`, `ferrosa-sstable/src/bin/ferrosa-sstable-import.rs:29`

## Issue

Two SSTable debugging/migration tools are stubbed:

1. `ferrosa-sstable-dump`: Should open SSTable components and iterate partitions for inspection. Currently just a TODO comment.

2. `ferrosa-sstable-import`: Should discover SSTable components in a source directory, read each table, and import into a target. Currently just a TODO comment.

## Impact

Operators cannot inspect or migrate SSTables outside the running database. Debugging production issues requires the full binary.
