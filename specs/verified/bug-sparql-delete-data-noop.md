# Bug: SPARQL DELETE DATA Doesn't Actually Delete

**Severity:** High
**Branch:** feat/sparql-endpoint
**File:** ferrosa-sparql/src/update.rs:79-88

## Issue

`DELETE DATA` parses correctly and increments `triples_deleted` counter but does not write tombstones or call storage.delete(). Data appears deleted (counter says so) but remains queryable.

## Impact

Silent data retention. Tests checking only the counter pass while data persists. Breaks referential integrity assumptions.

## Fix

Mirror the INSERT DATA logic but call `storage.delete_triple()` or equivalent tombstone write instead of incrementing a counter.

## Estimated Effort

20 minutes.

## Verification (2026-04-05)

Tested against feat/sparql-endpoint (commit 4a361b6):
- `/sparql/update` endpoint exists and accepts SPARQL UPDATE syntax
- But fails with "table not registered: rdf.rdf_triples" — the RDF triple store table hasn't been created
- INSERT DATA and DELETE DATA both fail at execution, not just DELETE
- Root cause appears to be missing DDL for the RDF triple store, not just the tombstone logic
- **Status: NOT FIXED** — broader issue than originally reported
## Verification Proof (2026-04-05)

Tested on feat/sparql-endpoint commit 8133168:
- INSERT DATA: {"triples_inserted":1} — works
- DELETE DATA: {"triples_deleted":1} — tombstones written
- Verified: data removed after DELETE
