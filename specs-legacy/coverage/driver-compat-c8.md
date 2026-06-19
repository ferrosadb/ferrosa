# C8 — Full CQL Driver Compatibility

**Status: COMPLETE.** All six language CQL drivers pass, the multi-protocol
python suite (CQL + graph/Cypher + Bolt + SPARQL + examples) is green, and an
auth-enabled node covers auth enforcement. Driver smoke (`run-all.sh`) is back to
**fail-loud**.

Final results (fresh nodes): node 38/0, go pass, java 38/0, rust 44/0, csharp
42/0, python 123 passed / 5 skipped (auth tests run on the auth node;
cassandra-example corpus optional), python-auth 4/4.

This file tracks the gaps surfaced and fixed along the way.

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

## Update (verified by local reproduction)

Reproducing each driver against a live node corrected the earlier picture:

- The "node 1/37" CI line was a **transient** (ferrosa not yet ready when node
  ran). Reproduced, the node driver gets **29 passed, 9 failed** — the *same*
  shared Ferrosa gaps as the other drivers, **not** a control-connection issue.
  So the gaps are shared CQL bugs, fixable once for all drivers.
- **Collections (item 2): FIXED** — bound collection bytes are now decoded via
  `types::decode_value` in `bridge.rs` instead of being rejected as blobs. Each
  driver drops ~6 failures (node 29/9 → **35/3**, verified end to end).
- **LWT (item 4): FIXED** for the rust driver in #72 (re-read the row instead of
  deserializing the variable-arity not-applied result).

### Remaining (each ~1 test/driver), precise root causes

| Gap | Root cause | Where |
|-----|-----------|-------|
| **NULL** | `INSERT ... null` materializes an empty-bytes live cell instead of a tombstone, so SELECT returns `""` not null (`Term::Null` → `CqlValue::Null` → insert path) | `router.rs` `materialize_insert` |
| **Batch** | bound `?` values are not substituted into batched statements, so a `BindMarker` reaches `term_to_cql_value` (which correctly rejects it); the BATCH protocol message *does* carry per-statement values, so this is a Ferrosa gap, not a test bug | `router.rs` `route_unlogged_batch` / `route_logged_batch` |
| **TTL** | expired cells are not filtered on read (compaction does not purge them either), so a TTL'd row survives past expiry | SELECT read path (storage) |

### Strict-driver round (gocql / DataStax Java / scylla-rust / csharp)

The shared NULL/batch/TTL/collections fixes landed (node 38/0). The strictly-typed
drivers then surfaced a deeper layer:

- **v5 protocol framing (go + java).** Both negotiate CQL v5 with the `USE_BETA`
  frame flag but **disagree on v5 framing**: gocql sends plain legacy 9-byte
  envelopes, the DataStax Java driver sends CRC24/CRC32 *modern* frames. No single
  server framing mode serves both at v5. **Resolution: ferrosa caps negotiation
  at v4** (`frame.rs` codec rejects v5+ with `supported = 4`), so every driver
  falls back to the one well-tested legacy transport. ferrosa's own `client.rs`
  now speaks v4 too.
- **v5 query/EXECUTE flags width.** v5 widened the query-parameters `<flags>`
  field from `[byte]` to `[int]`; `substitute_bound_values` now reads the width
  by protocol version (kept for correctness even though the v4 cap means v4 is
  used in practice).
- **PREPARE result metadata typing.** `build_result_columns` typed every function
  call and unknown system column as `varchar`, so prepared `COUNT(*)` (→ should be
  `bigint`) and `SELECT ... FROM system.local` (→ 0 columns) broke strict drivers
  that unmarshal by prepared metadata. Now typed to match `build_column_info` and
  the system.local/peers handlers.
- **java test bug:** `.withLocalDatacenter("dc1")` ≠ ferrosa's advertised
  `datacenter1` → "No node was available". Fixed in the test.

Status: **node 38/0, go PASS** end-to-end. java/csharp/rust re-verification in
progress.

Driver smoke stays informational (`continue-on-error` in `driver-tests.yml`,
asserted by `tests/ci/test_driver_smoke_workflow.py`) until all six drivers pass.
Flip both back to fail-loud once they do.
