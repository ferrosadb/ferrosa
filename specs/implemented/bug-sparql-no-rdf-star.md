# Bug: SPARQL Endpoint Missing RDF* Annotation Support

**Severity:** Medium (feature gap)
**Branch:** feat/sparql-endpoint
**File:** entire ferrosa-sparql crate (missing rdf_star.rs module)

## Issue

RDF* triple annotation syntax `<< ?s ?p ?o >> ?prop ?val` is not handled. No `rdf_star.rs` module exists. Queries with RDF* syntax fail with parse error.

## Impact

Cannot query edge provenance, confidence scores, or creation metadata via SPARQL. The ferrosa-memory eval framework's L3 semantic scenarios require this for edge provenance verification.

## Prerequisite

The `edge_annotations` CQL table and `annotation_put`/`annotation_get`/`annotation_list` Storage trait methods already exist in ferrosa-memory-core (implemented in Batch 2).

## Fix

Add `rdf_star.rs` module to ferrosa-sparql. Translate `<< ?s ?p ?o >> ?prop ?val` patterns to joins between `typed_edges` and `edge_annotations` tables. Support FILTER on annotation values.

## Estimated Effort

3-5 days (new module with planner + executor integration).

## Verification (2026-04-05)

Tested against feat/sparql-endpoint (commit 4a361b6):
- No rdf_star.rs module in crate
- RDF* syntax not handled
- **Status: NOT FIXED**

## Re-verification (2026-04-05, commit 8133168)

rdf_star.rs module exists (114 lines) but spargebra parser rejects RDF* syntax:
"error at 1:50: expected Reified triples are only available in SPARQL 1.2"
Fix: upgrade spargebra to version with SPARQL 1.2 support, or implement pre-processing
**Status: NOT FIXED** — parser limitation, not missing implementation

## Root Cause

`spargebra` 0.4 does not support RDF* (SPARQL-star) quoted triple syntax. The `<<` `>>` delimiters are rejected at parse time with "Reified triples are only available in SPARQL 1.2".

The `rdf_star.rs` module (114 lines) exists in the crate and has the join logic for edge_annotations, but it never gets called because the parser rejects the query before it reaches the planner.

## Recommended Fix (2 options)

### Option A: Pre-processing (faster, ~1 day)
Intercept the raw SPARQL string before passing to spargebra. Rewrite RDF* patterns into standard SPARQL with explicit joins:

```
# Input (RDF*):
SELECT ?s ?p ?o ?conf WHERE {
    << ?s ?p ?o >> ex:confidence ?conf .
    FILTER(?conf > 0.8)
}

# Rewritten (standard SPARQL):
SELECT ?s ?p ?o ?conf WHERE {
    ?s ?p ?o .
    ?_ann_s = ?s . ?_ann_p = ?p . ?_ann_o = ?o .
    ?_ann ex:annotates_src ?_ann_s .
    ?_ann ex:annotates_pred ?_ann_p .
    ?_ann ex:annotates_dst ?_ann_o .
    ?_ann ex:confidence ?conf .
    FILTER(?conf > 0.8)
}
```

Or more practically, detect `<< ... >>` patterns with regex, extract (s, p, o, prop, val), and inject a `ScanAnnotations` plan step directly in the planner (bypassing spargebra for that pattern).

### Option B: Upgrade/fork spargebra (~3 days)
- Fork spargebra and add SPARQL 1.2 quoted triple support
- Or switch to `oxigraph`'s parser which has partial SPARQL-star support
- Or use `sparesults` + custom parser for the RDF* subset

### Option C: Custom parser for RDF* subset (~2 days)
- Keep spargebra for standard SPARQL
- Add a pre-pass that extracts `<< ?s ?p ?o >> ?prop ?val` patterns before spargebra sees them
- Replace with placeholder variables, parse with spargebra, then inject annotation joins in the planner

## Files to Modify

- `ferrosa-sparql/src/engine.rs` — add pre-processing step before spargebra parse
- `ferrosa-sparql/src/planner.rs` — handle annotation join plan steps
- `ferrosa-sparql/src/rdf_star.rs` — already has join logic, needs to be wired in
- `ferrosa-sparql/src/executor.rs` — execute annotation scans against edge_annotations table

## Test Cases

1. `SELECT ?conf WHERE { << <http://t/a> <http://t/link> <http://t/b> >> <http://t/confidence> ?conf }` — returns confidence value
2. `SELECT ?s ?o ?who WHERE { << ?s ?p ?o >> <http://t/created_by> ?who }` — returns provenance
3. `SELECT ?s ?o WHERE { << ?s ?p ?o >> <http://t/confidence> ?c . FILTER(?c > 0.8) }` — filtered annotations
4. Standard SPARQL (no RDF*) still works unchanged after fix
