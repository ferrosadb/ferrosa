---
type: bug
priority: P2
reported-by: ferrosa-memory launch testing
implemented-by: ""
verified-by: ""
created: 2026-04-17
---

# `system_schema` SELECT ... WHERE predicate returns rows that don't match

## Observed

Running either of these against a freshly-created keyspace `agent_memory_test` that does NOT contain an `entity_store` table returned at least one row:

```sql
SELECT table_name FROM system_schema.tables
  WHERE keyspace_name = 'agent_memory_test' AND table_name = 'entity_store';

SELECT keyspace_name FROM system_schema.keyspaces
  WHERE keyspace_name = 'agent_memory_test';
```

The ferrosa-memory-mcp migration runner's adoption heuristic (look for `entity_store` as proxy for "is this a legacy pre-versioning keyspace?") tripped because the server-side filter didn't narrow the result set. Client-side filtering on the returned `keyspace_name` / `table_name` column was required to get correct behavior.

## Expected

`WHERE col = value` on `system_schema.*` tables should filter as Cassandra does. At minimum, `keyspace_name` (the partition key on most system_schema tables) should be a cheap exact-match predicate.

## Impact on ferrosa-memory

Low. Fixed with a client-side filter in `crates/ferrosa-memory-core/src/migration.rs::keyspace_exists`. Other callers of `system_schema` in fmem should audit for the same assumption — there's no central helper today.

## Reproduction

```bash
cd ferrosa-memory
scripts/start-test-cluster.sh
cat <<'CQL' | cqlsh localhost 19542  # or any CQL client
CREATE KEYSPACE agent_memory_test
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
SELECT keyspace_name FROM system_schema.keyspaces
  WHERE keyspace_name = 'definitely_not_a_real_keyspace';
CQL
```

If the second SELECT returns any rows at all (and the keyspace truly doesn't exist), this bug reproduces.

## Suggested investigation

- Confirm whether Ferrosa's `system_schema.tables` and `system_schema.keyspaces` honor `WHERE keyspace_name = ?` at the query planner, or only return the full partition and rely on the driver to filter.
- If the predicate is silently dropped rather than applied, that's the bug — push it down or return an error (`ALLOW FILTERING required`) instead of silently returning all rows.

## Workaround

Client-side match on the returned column, e.g.:

```rust
let envelope = session.query("SELECT keyspace_name FROM system_schema.keyspaces").await?;
let rows = envelope.response_body()?.into_rows().unwrap_or_default();
let exists = rows
    .into_iter()
    .filter_map(|r| r.r_by_name::<String>("keyspace_name").ok())
    .any(|n| n == target_keyspace);
```

## Implementation Notes

Added WHERE equality filtering to the three system_schema handlers that were missing it, following the existing pattern from the `columns` handler (which already implemented filtering correctly):

- `system_schema.keyspaces` (router.rs:600) — filters on `keyspace_name`
- `system_schema.tables` (router.rs:637) — filters on `keyspace_name`, `table_name`
- `system_schema.indexes` (router.rs:877) — filters on `keyspace_name`, `table_name`, `index_name`

New tests: `system_schema_keyspaces_where_filters_rows`, `system_schema_tables_where_filters_rows` — both verify that a WHERE clause for a nonexistent keyspace returns 0 rows.
