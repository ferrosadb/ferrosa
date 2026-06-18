---
title: Fail-Loud Storage Scan Contract — table-absent must be an Err, not an empty stream
status: todo
executive_summary: >
  Audit and fix `ferrosa-storage`'s `range_iter_projected` (and the sibling
  `range_read_projected`) so that a MISSING table surfaces as an explicit error
  (`NoSuchTable`), never a silent empty stream / `Ok(vec![])`. Today an unregistered
  table is indistinguishable from a legitimately empty one — a silent fallback the
  project's fail-loud rule forbids. The bespoke Postgres read path (`ferrosa-sql`
  binder/scan/join) MUST NOT inherit this behavior: a catalog-known but
  storage-unregistered table (timing / partial DDL broadcast / the D8 mapping race)
  would otherwise make a JOIN silently drop one side and return wrong/empty rows
  presented as success. This work item GATES the bespoke read path: the scan contract
  must distinguish table-absent (Err) from table-empty (Ok, zero rows) before
  `ferrosa-sql` scans on top of it. Tracks FMEA FM-41 and risk-register R15.
---

# Fail-Loud Storage Scan Contract

## Problem (ground truth)

`StorageEngine::range_iter_projected(table_id, …)` returns
`futures::stream::empty()` when the table is not registered, and
`range_read_projected` is documented to return `Ok(vec![])` for an unknown table.
That is a textbook silent fallback: "return empty when the operation could not be
performed." A query against a table that is **present in the catalog but not (yet)
registered in storage** — because of timing, a partial DDL broadcast, or the D8
keyspace↔database mapping race (FM-36) — returns **zero rows instead of an error**,
indistinguishable from a legitimately empty table. For a JOIN this silently drops
one side and produces a plausible-but-wrong result: a fresh instance of the dominant
FM-12 silently-wrong-result class, introduced not by the join algorithm but by the
storage contract beneath it.

This violates the project's fail-loud rule (`safety.md`, "Failure Philosophy: Fail
Loud, Never Fake" — never return `Ok(empty_result)` when the operation could not be
performed).

## Required contract

The engine scan contract MUST distinguish:

- **table-absent** → an `Err` (e.g. `NoSuchTable` / `NoSuchTable(table_id)`), carrying
  enough context to diagnose (table id, and that the table was catalog-known but
  storage-unregistered where applicable);
- **table-empty** → an `Ok` yielding **zero rows**.

The bespoke `ferrosa-sql` binder resolves table existence against the catalog AND
asserts storage registration **before** scanning; a catalog-present /
storage-absent table is fail-loud (mapped to `3D000` / `42P01`, or `XX000` with
context if it is an internal inconsistency), never an empty scan.

## Scope of work

1. **Audit** every caller of `range_iter_projected` / `range_read_projected` in
   `ferrosa-storage` (and re-exports through `ferrosa-cluster::WritePath` and the
   CQL read path) to find code that relies on the current empty-on-missing
   behavior; changing the contract must not silently break or alter CQL semantics
   (the CQL path's expectations must be preserved or explicitly updated with tests).
2. **Change the signature/behavior** so a missing/unregistered table returns
   `Err(NoSuchTable …)` rather than an empty stream / `Ok(vec![])`. Keep
   table-empty as `Ok` zero-rows.
3. **Map** the new error to a fail-loud outcome at the Postgres dispatch boundary
   (`3D000` / `42P01` for a missing relation; `XX000` + loud log for a
   catalog/storage inconsistency), coordinated with the SQLSTATE map work (R5).
4. **Wire into FM-36** (mapping-race) coverage so attach-then-immediately-query
   across nodes either sees the table or gets a clean error — never silent empty.

## Tests (TDD, fail-loud)

- Query a table that is **catalog-known but storage-unregistered** → assert an
  ERROR (`NoSuchTable` → SQLSTATE `42P01`/`3D000`), NOT empty rows.
- Query a **legitimately empty** (registered) table → assert zero rows, no error.
  The two outcomes are distinct and both asserted.
- JOIN where one side resolves to a catalog-known/storage-unregistered table →
  assert the JOIN errors, never silently drops that side.
- Regression: the existing CQL read path keeps its current behavior (or its
  changed behavior is covered by an explicitly updated test) — the CQL suite stays
  green.

## Gating / dependencies

- **Gates the bespoke Postgres read path** (`ferrosa-sql` scan/binder/join): the
  fail-loud contract must land (or be guaranteed at the binder) before the engine
  scans on top of `ferrosa-storage`.
- Relates to **FM-41** (FMEA) and **R15** (risk-register), and feeds **FM-36**
  (D8 mapping-race) coverage.
- Cross-repo note: `range_iter_projected` lives in the live `ferrosa-storage` crate
  (not the proposed front-end); this is a real engine-contract change, sequenced
  with care against the shipping CQL path.
