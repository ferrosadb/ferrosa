# Bug: SPARQL Property Paths Silently Degrade to Single Hop

**Severity:** Medium
**Branch:** feat/sparql-endpoint
**File:** ferrosa-sparql/src/planner.rs:259-287

## Issue

Property path operators (`+`, `*`, `?`) are recognized but silently degraded to single-hop evaluation. `?s foaf:knows+ ?o` extracts just `foaf:knows` and evaluates as a single triple pattern. Log warning emitted but no error returned to client.

## Impact

Queries return incomplete results without any indication of failure. Users expect transitive closure but get single-hop. Particularly dangerous for security queries ("who can access X transitively?").

## Fix

Option A: Return error for unsupported path operators until server-side BFS/DFS is implemented.
Option B: Implement transitive closure via graph engine's BFS/DFS internal API (see specs/sparql-endpoint-architecture.md prerequisite #3).

## Estimated Effort

Option A: 10 minutes. Option B: 2-3 days.

## Verification (2026-04-05)

Tested against feat/sparql-endpoint (commit 4a361b6):
- `SELECT ?s ?o WHERE { ?s <http://test/knows>+ ?o }` returns empty results without error
- No error or warning returned to client
- Property path `+` silently degraded to single hop
- **Status: NOT FIXED**
## Verification Proof (2026-04-05)

Tested on feat/sparql-endpoint commit 8133168:
- `<link>+` query: returns both b (1-hop) and c (2-hop via BFS)
- Transitive closure working correctly
