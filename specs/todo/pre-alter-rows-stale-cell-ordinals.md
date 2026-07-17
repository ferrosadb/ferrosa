---
status: todo
created: 2026-07-16
updated: 2026-07-16
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

## Repro sketch

1. `CREATE TABLE t (pk text PRIMARY KEY, name text, zz text)` → ordinals
   `name=0, zz=1`.
2. `INSERT INTO t (pk, name, zz) VALUES ('a', 'John', 'x')` — cells tagged
   `(0,'John'), (1,'x')`.
3. `ALTER TABLE t ADD aaa text` → new layout `aaa=0, name=1, zz=2`.
4. `SELECT name FROM t WHERE pk='a'` — the pre-ALTER cell tagged `0` now maps
   to `aaa`, so `name` reads back NULL (or `aaa` reads back `'John'`).

## Candidate fixes

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

- The repro above returns `'John'` for `name` after the ALTER, for memtable
  rows, replayed commit-log rows, and flushed SSTable rows.
- Index backfill over pre-ALTER SSTables builds postings from the correct
  column.
- A live-cluster test covers ALTER-then-read-old-rows across a restart.
