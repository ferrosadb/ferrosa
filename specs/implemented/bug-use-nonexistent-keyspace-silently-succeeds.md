---
type: bug
priority: P3
reported-by: ferrosa-memory launch testing
implemented-by: ""
verified-by: ""
created: 2026-04-16
updated: 2026-04-16
implemented-by: claude-code
source: ferrosa-memory-mcp migration runner
---

# USE against a nonexistent keyspace silently succeeds

## Observed

`USE agent_memory;` executed against a cluster that has no `agent_memory` keyspace returned success. The subsequent `ALTER TABLE entity_store ...` on the same session then failed with `"no keyspace specified"`.

Inferred: `USE` either (a) silently accepted the command without switching the session's default keyspace, or (b) silently unset the current default. Either way the client has no signal that the USE was invalid.

## Expected

`USE nonexistent_keyspace;` should return an error like Cassandra's `InvalidRequest: Keyspace 'X' does not exist`. This lets clients fail fast instead of discovering the problem two statements later.

## Reproduction

1. Start a fresh Ferrosa cluster with a keyspace `foo_test` (but NOT `bar_dev`).
2. Connect via cdrs-tokio.
3. Execute `USE bar_dev;` — observe success.
4. Execute `SELECT * FROM some_table;` — observe `"no keyspace specified"` error.

Alternatively, reproduce inside the ferrosa-memory-mcp test cluster: `docker-compose.test.yml` creates `agent_memory_test` but no `agent_memory`. Running the fmem migration runner before the fix in ferrosa-memory commit (the split_cql change that filters USE statements) hit this exact path — see that branch's history for evidence.

## Priority

P3. Low impact — clients should use fully-qualified table names and not rely on USE for migration scripts. Ferrosa-memory already switched to a runner that pins the keyspace explicitly and filters USE from DDL files. This bug surfaced the fmem mistake more slowly than it should have; a loud error would have saved debugging time.

## Suggested fix

In the CQL parser / controller for `USE`, validate the keyspace exists before accepting. Return `InvalidRequest` on unknown keyspace.

## Impact on ferrosa-memory

None going forward — the fmem migration runner now owns the keyspace context and never executes unqualified table references. Filing so the behavior is on record and other clients don't hit it.

## Implementation Notes

One-line fix: added `validate_keyspace_exists(&state.schema, &u.keyspace)?;` before the `SetKeyspace` result in `ferrosa-cql/src/router.rs:274`. The validation function already existed (used by INSERT/SELECT) but was not called for USE.

- System keyspaces (system, system_schema, etc.) bypass validation and always succeed.
- Updated `use_sets_default_keyspace` test to USE a system keyspace (was relying on the buggy behavior).
- New test: `use_nonexistent_keyspace_returns_error` confirms the fix.
