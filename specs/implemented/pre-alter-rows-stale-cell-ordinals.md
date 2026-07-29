---
status: implemented
created: 2026-07-16
updated: 2026-07-17
severity: P1
crates: [ferrosa-storage, ferrosa-sstable, ferrosa-cql]
related: [ST-16]
---

# Rows written before `ALTER TABLE ADD` carry stale cell ordinals

## Problem

Row cells are stored as `(u16 column_ordinal, CellValue)` where the ordinal is
the column's position within the schema's `regular_columns` **at write time**.
Regular columns are ordered by the column-name comparator, so
`ALTER TABLE ADD <col>` where `<col>` sorts before existing columns re-numbers
the ordinals of every column that sorts after it.

ST-16 fixed the *index-declaration* side of this (`TableStore::update_schema`
now remaps positional index declarations by column name). But rows written
**before** the ALTER still hold cells tagged with the old ordinals — in the
active memtable, in the commit log, and in flushed SSTables. Any read that
interprets those cells against the post-ALTER schema misattributes values to
the wrong columns: `SELECT` can return values under the wrong column, filters
match against the wrong data, and index rebuilds/backfills over old SSTables
build postings from the wrong cells.

## Resolution

Implemented in `ferrosa-storage`.

- `StorageEngine::update_table_schema` now holds the table-map write lock across
  an old-schema flush barrier before installing the new schema. The write path
  takes the table-map read lock for memtable admission, so the hold excludes any
  new row from entering the memtable between the flush and the schema swap. The
  flushed SSTable's `SerializationHeader` preserves the write-time ordinal
  layout, and the covered commit-log position is discarded after the flush.
- **Known residual (t_237efb08)**: the write path appends to the commit log
  *before* taking the table-map read lock, so a writer that has already
  appended an old-ordinal row and then blocks on the ALTER's write lock inserts
  that row into the new-schema memtable after the swap — and its commit-log
  position postdates the discarded barrier position, so it also replays
  mis-tagged after a crash. This closes the dominant window (all buffered rows)
  but not the racing-writer window; candidate closures are schema-epoch
  validation at memtable admission, moving the append under the guard, or
  name-keyed cells.
- Existing flushed SSTables continue to be read through their stored
  serialization header, so physical ordinals are remapped to the current schema
  by column name.
- Index backfill now maps a current regular-column ordinal and filtered-index
  predicate clauses through each SSTable's stored header before scheduling the
  build job. Backfills for columns absent from a legacy SSTable are marked
  indexed with no postings for that SSTable.

Tests:

- `engine::update_table_schema_preserves_unflushed_pre_alter_row_ordinals`
- `store::index_backfill_maps_current_ordinals_to_legacy_sstable_source_ordinals`
- Existing ST-16 pins:
  `store::update_schema_remaps_index_ordinal_when_added_column_sorts_first` and
  `ferrosa-cql` `router::tests::phonetic_keyed_equality_matches_when_fulltext_shares_the_column`

## Repro sketch

1. `CREATE TABLE t (pk text PRIMARY KEY, name text, zz text)` → ordinals
   `name=0, zz=1`.
2. `INSERT INTO t (pk, name, zz) VALUES ('a', 'John', 'x')` — cells tagged
   `(0,'John'), (1,'x')`.
3. `ALTER TABLE t ADD aaa text` → new layout `aaa=0, name=1, zz=2`.
4. `SELECT name FROM t WHERE pk='a'` — the pre-ALTER cell tagged `0` now maps
   to `aaa`, so `name` reads back NULL (or `aaa` reads back `'John'`).

## Candidate fixes considered

- **Cell-ordinal versioning**: stamp each row (or memtable/SSTable generation)
  with a schema epoch; readers remap ordinals from the row's epoch layout to
  the current layout by column name (Cassandra solves this by serializing
  column identity, not position).
- **Rewrite-on-ALTER**: eagerly remap active memtable rows at
  `update_schema` time and remap SSTable cells during compaction (the flushed
  metadata already records the writing schema's column list per SSTable —
  verify).
- **Ordinal stability**: never re-sort existing regular columns — assign new
  columns the next free ordinal instead of resorting (diverges from
  Cassandra's comparator ordering; audit every consumer of
  `regular_columns` order first, including SSTable serialization
  compatibility).

## Acceptance

- Done: the repro above returns `'John'` for `name` after the ALTER for
  unflushed rows and after a restart/replay boundary, because dirty rows are
  flushed under the old schema before the schema swap.
- Done: flushed SSTable rows are interpreted through the SSTable
  serialization header rather than the current ordinal layout.
- Done: index backfill over pre-ALTER SSTables builds postings from the correct
  source column, and skips absent newly added columns for legacy SSTables.
- Remaining evidence gap: a live-cluster ALTER-then-read-old-rows smoke can
  still be useful, but the storage-layer invariant is covered in-crate.
