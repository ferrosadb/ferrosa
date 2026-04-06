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
