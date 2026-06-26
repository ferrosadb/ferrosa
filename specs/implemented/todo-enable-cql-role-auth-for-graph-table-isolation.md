---
type: todo
priority: P2
status: draft
created: 2026-04-20
updated: 2026-04-20
---

# Enable CQL role-based auth so graph-internal tables are off-limits to application writers

## Why

ferrosa's current deployment on the ferrosa-memory cluster runs with
`FERROSA_AUTH_DISABLED=true` — `ferrosa/src/web/auth.rs::auth_middleware`
is a no-op, and the CQL layer accepts any connection. Any process on the
podman-internal network can write to any table.

That made the bypass documented in
`bug-ferrosa-memory-bypasses-graph-api-for-writes.md` possible. Even
after the ferrosa-memory code is fixed, there's no enforcement stopping
the next client from making the same mistake.

## Proposed

Wire up CQL role-based auth end-to-end, plus per-table grants, so the
DB itself refuses direct writes to graph-internal tables regardless of
client correctness.

1. **Roles** in ferrosa (per-table grants via the existing system-auth
   tables; confirm the `system_auth` keyspace + `roles`/`role_permissions`
   tables are functional):
   - `graph_engine` — the identity ferrosa-graph uses when it writes
     through the Cypher executor (requires plumbing inside ferrosa).
     Has `MODIFY` on `agent_memory.typed_edges` and siblings.
   - `app_reader` — `SELECT` on graph tables, `MODIFY` on
     application-owned tables (see
     `todo-split-keyspaces-application-vs-graph.md`).
   - `ops` — admin role for snapshot/compaction/cluster control via
     the `:9090` web API.

2. **Client auth credentials** rolled into secrets volumes that each
   consumer mounts (ferrosa-memory, loadgen, backup-memory.sh, etc.).

3. **Flip `FERROSA_AUTH_DISABLED=false`** on all three nodes in
   `docker-compose.yml` — already wired into the config, just disabled
   for dev convenience.

4. **Fail-closed migration**: stage a `FERROSA_AUTH_WARN=true` mode
   first — log every would-be-denied request for a few days so
   unexpected consumers surface before enforcement flips on.

## Acceptance criteria

- [ ] `FERROSA_AUTH_DISABLED=true` is no longer set in any shipped
      compose/deploy config.
- [ ] Direct `INSERT` on `typed_edges` from a non-`graph_engine` role
      fails with `Unauthorized`.
- [ ] Integration test in `ferrosa-cluster/tests/` that authenticates
      as `app_reader`, tries to `INSERT` into `typed_edges`, and
      asserts the error.
- [ ] `ferrosa-memory` + `backup-memory.sh` + loadgen all connect and
      work under the new auth config (their own roles, own grants).

## Related

- `bug-ferrosa-memory-bypasses-graph-api-for-writes.md`
- `todo-extend-ferrosa-memory-graph-client-with-cypher-writes.md`
- `ferrosa/src/web/auth.rs` — middleware to un-bypass on the web side.
