# Bug: Ghost Rows with All-Null Columns in CQL Tables

**Severity:** High (data corruption)
**Component:** ferrosa-storage
**Related:** bug-drop-table-does-not-delete-data.md (likely same root cause)

## Issue

CQL tables contain rows where ALL regular columns are null — including columns that are part of the primary key's clustering columns. These "ghost rows" are not inserted by any client. They appear in `entity_store`, `typed_edges`, and potentially other tables.

Example from `typed_edges`:
```
src_id=None, edge_type=None, dst_id=None, weight=None, created_at=None
```

Example from `entity_store`:
```
entity_id=None, entity_name=None, entity_type=""
```

The partition key columns (`tenant_id`, `session_id`) are present (row is queryable), but ALL clustering columns and regular columns are null.

## Impact

- ANN search crashes on null entity_id (previously fixed with `bf74e93` skip logic)
- Edge traversal returns null endpoints, causing panics or empty results
- `count(*)` inflated by ghost rows
- Clients must defensively filter nulls on every query

## Evidence

9 ghost rows in `typed_edges`, at least 1 in `entity_store`, observed after `frg ingest` + `run_consolidation` on a fresh cluster. The ingest tool does not INSERT null values.

## Likely Cause

Related to the DROP TABLE data retention bug. Ferrosa's tombstone/compaction system may be:
1. Creating placeholder rows during replication without populating columns
2. Failing to fully delete rows, leaving null-column remnants
3. Replaying partial writes from the commit log during crash recovery

## Reproduction

```bash
# Fresh cluster
podman compose up -d
# Ingest data
frg ingest --cql localhost:19042 .
# Check for ghosts
SELECT * FROM typed_edges WHERE tenant_id = ? AND session_id = ? LIMIT 5;
# First rows often have all-null columns
```

## Workaround

Filter nulls defensively in all query results:
```rust
if r.src_id.is_none() || r.dst_id.is_none() { continue; }
```

This is already done in some places (`bf74e93`) but needs to be systematic.
