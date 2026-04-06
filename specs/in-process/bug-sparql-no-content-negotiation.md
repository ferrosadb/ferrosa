# Bug: SPARQL Endpoint Ignores Accept Header

**Severity:** Medium
**Branch:** feat/sparql-endpoint
**File:** ferrosa-sparql/src/http.rs:153

## Issue

The `/sparql` endpoint always returns `application/sparql-results+json` regardless of the `Accept` header. Clients requesting `text/turtle` or `application/n-triples` get JSON.

## Impact

Spec non-compliance. Clients requiring Turtle or N-Triples format cannot use this endpoint. Breaks standard SPARQL client libraries that rely on content negotiation.

## Fix

Parse `Accept` header, dispatch to appropriate formatter in results module. Default to JSON for SELECT/ASK, Turtle for CONSTRUCT.

## Estimated Effort

30 minutes.
