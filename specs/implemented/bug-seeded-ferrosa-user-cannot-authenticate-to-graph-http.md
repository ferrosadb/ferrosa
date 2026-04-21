---
type: todo
priority: P1
status: draft
created: 2026-04-20
updated: 2026-04-20
---

# Bug: seeded `ferrosa_user` credentials cannot authenticate to graph HTTP

## Why this is a Ferrosa bug

The expected rollout contract is that Ferrosa seeds:

- `ferrosa_admin / ferrosa_admin`
- `ferrosa_user / ferrosa_user`

as the default admin and normal-user credentials.

If the seeded normal-user credentials are documented and expected to
exist, the public graph HTTP surface should authenticate them
successfully and then enforce permissions based on that identity.

Current behavior is an authentication failure, not a permission denial.

## Observed on

- Ferrosa commit: `6fce814`
- Endpoint: `POST /graph/query`
- URL: `http://127.0.0.1:17474/graph/query`

## Repro

```bash
curl -sS -u ferrosa_user:ferrosa_user \
  -X POST 'http://127.0.0.1:17474/graph/query' \
  -H 'Content-Type: application/json' \
  --data '{"query":"MATCH (n:Entity) RETURN n.entity_id LIMIT 1","keyspace":"agent_memory"}'
```

## Actual

```json
{"error":"authentication failed"}
```

## Expected

One of these:

1. `ferrosa_user` authenticates successfully and the query executes if
   it is allowed for a normal user.
2. `ferrosa_user` authenticates successfully and receives a permission
   error if the operation is not allowed.

Authentication itself should not fail if these are truly seeded public
credentials.

## Control probes

- `ferrosa_admin:ferrosa_admin` authenticates and can issue graph
  mutations.
- `app_reader:ferrosa_user` authenticates and is denied `MERGE` with
  `{"error":"permission denied"}`, which is the correct shape for a
  permission failure.

That makes the missing/invalid `ferrosa_user` login path stand out as a
separate issue.

## Impact

- Client and operator docs cannot rely on the seeded normal-user
  credential contract.
- Any smoke tests or demos that use `ferrosa_user` fail before authz is
  even exercised.

## Acceptance

- `ferrosa_user:ferrosa_user` authenticates on graph HTTP.
- Follow-up requests fail or succeed based on permissions, not auth
  absence.

## Implementation Notes

Root cause: `ferrosa-schema/src/auth/bootstrap.rs` seeded three roles —
`ferrosa_admin`, `graph_engine`, `app_reader`. The `app_reader` role held
the "unprivileged normal user" grant matrix with password `ferrosa_user`.
The user-facing contract (per this bug and the design doc) expected the
role NAME to be `ferrosa_user`, not `app_reader`.

Fix: renamed the seeded role from `"app_reader"` to `"ferrosa_user"` in
one place — the `SEED_APP_READER_USER` constant in `bootstrap.rs:66`.
That constant is the single source of truth; the 12 call sites that use
it (bootstrap, tests, the graph HTTP auth middleware) automatically pick
up the new name. String-literal `"app_reader"` references in three test
files (`auth_isolation.rs`, `auth_integration.rs`, `auth_warn_mode.rs`)
were updated to `"ferrosa_user"`. Added a preferred-name alias constant
`SEED_APP_USER = SEED_APP_READER_USER` for new callers.

Tests added (all green via `cargo +stable test -p ferrosa-schema`):
- `seeded_ferrosa_user_role_exists_after_bootstrap`
- `seeded_ferrosa_user_authenticates_with_default_password`
- `seeded_ferrosa_user_has_select_on_graph_tables`
- `seeded_ferrosa_user_does_not_have_modify_on_graph_tables`

Regression pass: `ferrosa-schema` (317/317), `ferrosa-cluster auth_isolation`
(8/8), `ferrosa-cql auth_integration + auth_warn_mode + handshake` (26/26).
