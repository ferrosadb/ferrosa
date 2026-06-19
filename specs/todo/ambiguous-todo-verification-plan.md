# Ambiguous TODO Verification Plan

> Last updated: 2026-05-13
> Status: Verification required before claiming done

The docs audit found several stale-looking work items with source evidence suggesting they may be implemented or partly implemented. They remain outside public claims until a clean checkout can run the listed checks and attach output.

## Items to Verify

| Item | Current docs location | Verification plan | Closure target |
|---|---|---|---|
| Startup smoke test for corrupt SSTables | `specs/todo/todo-startup-smoke-test-for-corrupt-sstables.md` | Run the corrupt-SSTable startup repro from the item on a clean local cluster; confirm startup fails loud without hiding corrupt files. | Move to `archive/bugs-verified/` if the repro passes; otherwise keep in `todo/`. |
| CQL role-auth for graph table isolation | `specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md` | Run graph-table write attempts as a non-graph role and as the graph service role; verify non-graph writes fail and graph writes succeed. | Move to `implemented/` or `archive/bugs-verified/` only with command output. |
| Graph write seam / public Cypher mutations | `specs/implemented/todo-implement-public-cypher-mutations-for-client-graph-writes.md` | Exercise public Cypher create/merge/update/delete paths and read back through graph queries plus backing CQL tables. | Archive once live graph read-after-write checks pass. |
| Repair and hinted-handoff topology tasks | `specs/todo/todo-hints-topology-change-wrong-node.md`, `specs/todo/todo-batchlog-remote-delete-replay-duplication.md` | Run multi-node topology-change and remote-delete replay scenarios; confirm hints/batchlog entries route to current owners only once. | Keep open unless live-cluster evidence passes. |
| Prepared SELECT metadata | No standalone current TODO file found; source/docs references mention prepared metadata and vector binding | Run scylla/cassandra driver prepared statements over vector and non-vector columns; verify result metadata and bind metadata match driver expectations. | Create or archive a concrete work item only after driver-level tests pass. |
