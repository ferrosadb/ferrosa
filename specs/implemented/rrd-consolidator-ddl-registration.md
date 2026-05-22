---
type: todo
priority: P0
status: implemented
created: 2026-05-20
updated: 2026-05-20
---

# Register RRD consolidators from DDL/schema rebuild

## Why

`CREATE TABLE ... WITH extensions = {'consolidation.*': ...}` currently
creates metadata, but it does not install active `TimeSeriesAggregator`
instances into `StorageEngine`.

## Acceptance Criteria

- Valid consolidation extensions register an active consolidator when schema is
  loaded or DDL is applied.
- Invalid consolidation extensions fail at DDL time with actionable errors.
- Dropping/altering a table unregisters or replaces the consolidator.
- Tests prove normal writes reach the consolidator without inline
  materialization.

## Implementation Notes

- `StorageEngine` now builds a DDL/schema-load time-series consolidator handle
  from valid `consolidation.*` table extensions.
- Consolidator handles retain the bounded task receiver and register the
  `TimeSeriesAggregator` as a sync observer, but writes still return no inline
  derived mutations.
- Numeric aggregation columns are validated against normalized Cassandra/CQL
  type names before the table registration/update takes effect.
- `DROP TABLE` and `ALTER TABLE` replacement remove the old observer; invalid
  ALTER replacements fail before the existing consolidator is removed.
- Coverage added in `ferrosa-storage/src/engine.rs` for explicit registration,
  local schema load, invalid metadata, drop cleanup, and alter replacement.
