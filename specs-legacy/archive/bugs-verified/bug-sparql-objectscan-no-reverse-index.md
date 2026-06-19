# Bug: SPARQL ObjectScan Performance — Missing Reverse Edge Index

**Severity:** Medium (performance)
**Branch:** feat/sparql-endpoint
**File:** ferrosa-sparql/src/executor.rs:346-348

## Issue

Queries binding only the object position (`?s ?p :bob`) perform a full table scan of up to 10,000 rows with post-fetch filtering. Results beyond the 10K cap are silently dropped.

The architecture spec (specs/sparql-endpoint-architecture.md) calls for a reverse-edge materialized view but it hasn't been created.

## Impact

- Incorrect results: queries miss data beyond 10K rows
- Poor performance: full scan instead of indexed lookup
- Silent data loss: no warning when results are truncated

## Fix

Create the reverse index:
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS agent_memory.typed_edges_by_dst AS
    SELECT * FROM agent_memory.typed_edges
    WHERE tenant_id IS NOT NULL AND session_id IS NOT NULL AND dst_id IS NOT NULL
    AND src_id IS NOT NULL AND edge_type IS NOT NULL
    PRIMARY KEY ((tenant_id, session_id, dst_id), edge_type, src_id);
```

Then update executor's ObjectScan to query the materialized view.

## Estimated Effort

1-2 days (DDL + executor update + testing).

## Verification (2026-04-05)

Tested against feat/sparql-endpoint (commit 4a361b6):
- Cannot verify directly (RDF triple store table not created, so no data to scan)
- ObjectScan uses post-fetch filter (workaround in place) but no reverse index materialized view
- **Status: NOT FIXED** (workaround exists but reverse index missing)
## Verification Proof (2026-04-05)

Tested on feat/sparql-endpoint commit 8133168:
- `?s <link> <c>` correctly returns b (and a from 2-hop)
- Reverse scan produces correct results
