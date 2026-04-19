---
type: bug
priority: P1
reported-by: ferrosa-memory production cluster
implemented-by: ""
verified-by: ""
created: 2026-04-18
updated: 2026-04-18
---

# Startup should quarantine SSTables with zero-byte component files

## Observed

Node1 has 29 SSTables — ALL with zero-byte Rows.db files (from the pre-fix writer bug). Every `read_range` query logs 29 WARN lines:

```
WARN ferrosa_storage::store: read_range: skipping corrupted SSTable:
  I/O error: read_exact_at: wanted 27497 bytes, got 8332
```

The data in these SSTables is unrecoverable (Rows.db is empty). The SSTables should be moved aside on startup so they don't spam warnings on every query.

## Fix

In `StorageEngine::load_existing_sstables_and_sidecars()`, validate each SSTable's component files before loading:
1. Check that Data.db, Partitions.db, and Rows.db all exist and are non-empty
2. If any component is zero-byte or missing, move the entire SSTable directory to a quarantine folder
3. Log ERROR once per quarantined SSTable with the generation ID

## Acceptance Criteria

- [ ] SSTables with zero-byte Rows.db are quarantined on startup
- [ ] No WARN spam from corrupted SSTables during steady-state queries
- [ ] Quarantined SSTables are preserved (not deleted) for investigation
