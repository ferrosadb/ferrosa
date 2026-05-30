# C8 — Full CQL Driver Compatibility

Driver smoke (`tests/drivers/run-all.sh`, six languages) is **informational**
(continue-on-error) until all drivers pass consistently. This file tracks the
gaps surfaced when the harness was made fail-loud.

Diagnosis from the 2026-05-30 run (per-driver: csharp 39/3, go 29/9, java 35/9,
node 1/37, rust/python failing):

## Real Ferrosa gaps

1. **node driver control connection** — the DataStax Node.js driver fails ~37/38
   tests. The default DC matches (`system.local` reports `datacenter1`,
   `ferrosa-schema/src/system/local.rs:50`), so it is not a DC-name mismatch.
   Likely a control-connection / metadata issue specific to that driver. Highest
   impact; fix first. Needs reproduction against a running node.
2. **Collection binding** — inserting a `list`/`set`/`map` is rejected with
   "type mismatch: expected list, got blob literal". A bound collection value is
   being treated as a blob instead of being decoded against the column's
   collection type. Affects go and others.
3. **TTL read expiry** — "TTL row should be gone after expiry": rows written with
   a TTL are not filtered out on read after expiry. (Note: compaction does not
   purge expired cells; expiry must be applied at read time.)

## Likely test-side bugs (fix in the driver tests)

4. **LWT not-applied result** — `INSERT ... IF NOT EXISTS` that is *not* applied
   returns `[applied=false]` plus the existing row's columns (Cassandra
   semantics). Tests deserialize to `(bool,)` and fail with "12 columns, rust
   types contains 1". Tests must handle the not-applied shape (or guarantee the
   row does not pre-exist).
5. **Batch bind markers** — bind markers in an *unprepared* batch are invalid
   CQL; Ferrosa correctly rejects them ("bind markers not supported in
   non-prepared queries"). Tests should prepare the statements or use literals.

## Plan

Fix the test-side bugs (4, 5) first to cut the failure count, then the Ferrosa
gaps in impact order: node control connection (1), collections (2), TTL (3).
Flip `driver-tests.yml`'s driver-smoke step back to fail-loud (and
`tests/ci/test_driver_smoke_workflow.py`) once green.
