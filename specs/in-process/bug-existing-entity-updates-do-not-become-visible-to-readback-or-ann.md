---
type: bug
priority: P1
status: in-process
created: 2026-04-23
updated: 2026-04-23
reported-by: codex
---

# Existing entity updates do not become visible to readback or ANN

## Summary

On the local three-node Ferrosa cluster used by `ferrosa-memory`, targeted updates against existing rows in `agent_memory.entity_store` appear to succeed, but the updated values do not become visible in follow-up readback and ANN retrieval still returns empty results for known older entities.

## Repro shape

Environment:

- Ferrosa cluster behind the local `18765/18766` `ferrosa-memory` stack
- tenant `9a5f8fbf-d842-4d30-8ea5-1aa931e618a8`
- `ferrosa-memory-batch` configured to use `nomic-embed-text-v2-moe`

Observed sequence:

1. `ferrosa-memory-batch backfill-rich-entities` runs Phase 0 successfully:
   - `p0_entities_embedded=7466`
   - `p0_failed=0`
2. The batch now uses a targeted update path:
   - `UPDATE entity_store SET entity_embedding = [...] WHERE tenant_id=? AND session_id=? AND entity_id=?`
   - `UPDATE entity_store SET updated_at = ? WHERE tenant_id=? AND session_id=? AND entity_id=?`
3. Fresh rows written through `ingest_entities` with `embed_missing=true` are retrievable via ANN on the live MCP server.
4. Older known rows remain problematic:
   - `SELECT session_id, entity_id, entity_name, updated_at FROM agent_memory.entity_store LIMIT 10`
     still shows old `updated_at` values for older entities like:
     - `complexity-audit` / `41753309-7297-454e-8f2d-c6546740cf2b`
     - `compile-project` / `c2d878e0-d67c-4b19-8277-18414c777382`
   - `retrieve_entities` with `strategy='ann'` for those older sessions returns `[]`

## Why this is a Ferrosa bug

- `ferrosa-memory` is issuing targeted CQL updates for existing rows and seeing no write errors.
- New writes are retrievable, so the client embedding and query path is not globally broken.
- The remaining failure is specifically visibility/index behavior for updates to pre-existing rows.

If the update path is accepted but does not affect readback or ANN visibility for existing rows, the database is not performing to the public contract.

## Expected

- A successful targeted update to `entity_embedding` and `updated_at` for an existing entity row must become visible to subsequent reads.
- ANN retrieval in the same `(tenant_id, session_id)` partition should include the updated entity once its embedding is present.

## Actual

- Update path reports success / no client error.
- Older rows still read back with stale `updated_at`.
- ANN retrieval for older rows remains empty.

## Notes

- `ferrosa-memory` has already been adjusted to avoid the broader `entity_put` upsert path during Phase 0 backfill, so this is no longer explainable as a stale-row rewrite bug in the client.
- A separate verification artifact may still exist in vector rendering for CQL passthrough, but the stale `updated_at` values and empty ANN results are sufficient to show the core update-visibility problem.
