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
