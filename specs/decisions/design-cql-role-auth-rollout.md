---
type: design
priority: P2
status: draft
for: specs/todo/todo-enable-cql-role-auth-for-graph-table-isolation.md
created: 2026-04-20
updated: 2026-04-20
---

# Design — Rollout of CQL role-based auth for graph-table isolation

Scoped blueprint for the `todo-enable-cql-role-auth-for-graph-table-
isolation.md` work item. This is enablement, not new design — the auth
primitives already exist in `ferrosa-schema` and the CQL parser.

## 1. Current state (what exists)

- **Schema-side primitives**:
  - `ferrosa-schema/src/auth/role.rs` — `RoleMetadata`, `AuthContext`
  - `ferrosa-schema/src/auth/permission.rs` — `has_permission`,
    `has_permission_recursive`, `Permission` enum
    (Create/Alter/Drop/Select/Modify/Authorize/Describe/Execute)
  - `ferrosa-schema/src/system/schema_tables.rs` — `system_auth.roles`,
    `role_permissions`, etc.
  - `ferrosa-schema/src/registry.rs` — `AuthMethod`
- **CQL parser**: `CREATE ROLE`, `GRANT`, `REVOKE`, `ALTER ROLE`,
  `DROP ROLE` already lex/parse. See `ferrosa-cql/src/parser.rs`.
- **CQL router**: has permission-check callsites
  (`ferrosa-cql/src/router.rs`).
- **Web auth middleware**: `ferrosa/src/web/auth.rs::auth_middleware`
  exists and is wired into the router; killed today by
  `FERROSA_AUTH_DISABLED=true`.
- **Gap — not yet done**:
  - No CQL-level authenticator plumbing at the connection layer
    (ferrosa accepts any `STARTUP` frame without credentials).
  - No seed-role bootstrap (no default `cassandra`/`admin` role
    created at fresh-cluster startup).
  - `FERROSA_AUTH_DISABLED=true` set in deployed `docker-compose.yml`.
  - No client-side credential mount in ferrosa-memory / loadgen /
    backup-memory.sh.

## 2. Target state

Three named roles, principle-of-least-privilege grants:

| Role | Keyspaces | Graph tables | App tables | System tables | Admin ops |
|---|---|---|---|---|---|
| `graph_engine` | agent_memory | MODIFY+SELECT | — | DESCRIBE | — |
| `app_reader` | agent_memory | SELECT only | MODIFY+SELECT | DESCRIBE | — |
| `ops` | * | * | * | * | ALTER/DROP; web `:9090` admin |

"Graph tables" = `typed_edges`, `folded_into`, `mentioned_in`,
`co_occurs_with`, `supersedes`, `derived_edges_by_pred`,
`derived_edges_by_src`. "App tables" = everything else in
`agent_memory`.

ferrosa-memory connects as `app_reader`. ferrosa-graph, when writing
through its Cypher executor, uses `graph_engine` internally. Human
operators and the backup script authenticate as `ops`.

## 3. Work breakdown (suggested sprints)

### Sprint A — server-side auth plumbing (2–3 days)

Enable `FERROSA_AUTH_DISABLED=false` without breaking anything.

1. **Seed role bootstrap.** On first startup under auth, create a
   default `cassandra`/`cassandra` superuser if `system_auth.roles` is
   empty. Log a loud warning if the default password is still in use
   after 5 minutes. New code in `ferrosa/src/main.rs` startup.
2. **CQL STARTUP/AUTHENTICATE frames.** Confirm ferrosa already
   responds with `AUTHENTICATE` when `auth_disabled=false`; wire
   through `PasswordAuthenticator` (SASL PLAIN) in the CQL server
   (`ferrosa-cql/src/server.rs`). Unit tests for wrong-password,
   unknown-role, disabled-role.
3. **Router-level permission enforcement.** Every CQL statement
   handler (SELECT/INSERT/UPDATE/DELETE/DDL) calls
   `has_permission(auth_ctx, required_perm, resource)`. Already a
   callsite exists in `router.rs`; audit coverage and close gaps.
4. **Web `:9090` auth**. Stop bypassing `auth_middleware` when
   `FERROSA_AUTH_DISABLED=true` — the current behavior is the whole
   middleware becomes a no-op. Instead: treat that flag as "allow
   anonymous read-only"; writes/admin still require a token.
5. **Audit log**. Every permission denial emits a
   `system_auth.audit_log` row. Already wired — just verify under
   load.

### Sprint B — role + grant seed (1–2 days)

6. **Migration file** `ddl/100_roles.cql` in ferrosa-memory:
   ```sql
   CREATE ROLE IF NOT EXISTS graph_engine
     WITH PASSWORD = '{{ graph_engine_password }}' AND LOGIN = true;
   CREATE ROLE IF NOT EXISTS app_reader
     WITH PASSWORD = '{{ app_reader_password }}' AND LOGIN = true;
   CREATE ROLE IF NOT EXISTS ops
     WITH PASSWORD = '{{ ops_password }}' AND LOGIN = true AND SUPERUSER = true;

   GRANT SELECT ON KEYSPACE agent_memory TO graph_engine;
   GRANT MODIFY ON agent_memory.typed_edges TO graph_engine;
   GRANT MODIFY ON agent_memory.folded_into TO graph_engine;
   -- … all graph tables …

   GRANT SELECT ON KEYSPACE agent_memory TO app_reader;
   GRANT MODIFY ON agent_memory.entity_store TO app_reader;
   GRANT MODIFY ON agent_memory.tool_usage_log TO app_reader;
   -- … all app tables, excluding the graph set …
   ```
7. **Secrets**. Passwords generated per-cluster, written to
   `~/data/ferrosa-memory/.runtime/secrets/{graph_engine,app_reader,ops}.env`,
   mounted read-only into containers. NOT committed.

### Sprint C — client updates (2 days)

8. **ferrosa-memory** reads `FERROSA_CQL_USER` + `FERROSA_CQL_PASSWORD`
   from env; passes to `cdrs-tokio`'s `PasswordAuthenticator`.
   Already supported by the driver — plumbing only.
9. **ferrosa-graph (server-side)**: when the Cypher executor writes to
   CQL tables, it authenticates internally as `graph_engine`. This is
   in-process, not network — probably a trait impl that selects an
   internal identity. Small code change.
10. **backup-memory.sh + loadgen**: same env-var pattern.

### Sprint D — warn-then-enforce migration (1 day of calendar, 3+ days of soak)

11. **Ship `FERROSA_AUTH_WARN=true` mode**. Auth is checked but denials
    only log a loud `WARN` — request still succeeds. Existing
    callsites at `permission.rs:121` gain a warn-vs-deny branch.
12. **Deploy + soak for 72 hours**. Collect `Unauthorized` warnings,
    fix any consumer that surfaces in the logs.
13. **Flip to enforce**. `FERROSA_AUTH_WARN=false` (or delete the var
    entirely), denials become real `Unauthorized` errors. Keep
    `FERROSA_AUTH_DISABLED` env var **deleted** from compose — don't
    let a future deploy re-silence it.

### Sprint E — verification (1 day)

14. **Integration test** in `ferrosa-cluster/tests/auth_isolation.rs`:
    - Authenticate as `app_reader`, attempt `INSERT INTO typed_edges …`
      → expect `Unauthorized`.
    - Authenticate as `graph_engine`, attempt the same → expect
      success.
    - Authenticate as `app_reader`, attempt
      `INSERT INTO entity_store …` → expect success.
15. **E2E smoke** against the ferrosa-memory cluster: ferrosa-memory's
    tool_usage_log writes, backup script, loadgen all succeed under
    the new roles.

## 4. Risk matrix

| Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|
| Lock out legitimate client during flip | High | Medium | `AUTH_WARN` soak; rollback = set `AUTH_DISABLED=true` |
| Seed credentials leaked in logs | High | Low | Never log passwords; secrets mounted from read-only volume |
| graph_engine's internal identity ambiguous after ferrosa restart | Med | Low | Bootstrap checks that `graph_engine` role exists before accepting writes |
| Permission check in hot path adds latency | Low | Low | Cache `AuthContext` per connection; measure in Sprint A |
| `system_auth.roles` replication lag after CREATE ROLE | Med | Low | Require quorum write for role DDL; fail-close if not replicated |

## 5. Estimate

| Sprint | Calendar days | Risk |
|---|---|---|
| A — server plumbing | 2–3 | Low (primitives exist) |
| B — role seed | 1–2 | Low |
| C — client updates | 2 | Low |
| D — warn+soak | 4 (72 h soak) | Medium |
| E — verification | 1 | Low |
| **Total** | **~10 calendar days** | **Medium overall** |

Most risk is in Sprint D — the soak could uncover unexpected
consumers. Budget a week of buffer if the dev cluster is also a
shared staging environment.

## 6. Out of scope (file as separate todos)

- **Per-query fine-grained grants** (e.g., `SELECT` on column-subset).
  Cassandra doesn't support and we shouldn't invent it.
- **LDAP/OIDC integration**. Password-file auth is enough for v1.
- **mTLS on CQL wire**. Separate hardening todo.
- **Split keyspace** (`agent_memory` vs `agent_graph`). Referenced by
  the original todo but is its own work item; file as
  `todo-split-keyspaces-application-vs-graph.md` when appropriate.

## 7. Rollback plan

If enforcement breaks production reads/writes after the flip:

1. Set `FERROSA_AUTH_DISABLED=true` in `docker-compose.yml`.
2. `podman compose restart node1 node2 node3`.
3. Effective within ~30 seconds; roles remain in place but unused.
4. Root-cause the denied request, add the missing GRANT, retry the
   enforcement flip.

The rollback is reversible and non-destructive.
