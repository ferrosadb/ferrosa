---
type: todo
priority: P3
status: draft
created: 2026-05-11
updated: 2026-05-11
---

# Bug: `PREPARE` rejects `?` bind markers on `system_schema.keyspaces`

## Why this is a Ferrosa bug

Ferrosa advertises Cassandra/Scylla wire-protocol compatibility. Both
reference drivers (Python `cassandra-driver`, Java `cassandra-driver-core`,
Rust `scylla`) support `PREPARE`d statements with `?` bind markers against
system tables — including `system_schema.keyspaces`. Ferrosa rejects this:

```
PREPARE failed for 'SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?':
expected 1 bind-marker column spec(s) but resolved only 0.
Table 'system_schema.keyspaces' may not yet be visible on this node (schema replication lag).
Retry in a moment.
```

The error message is misleading — the keyspace IS visible (the same query
with a literal value succeeds, and `SELECT keyspace_name FROM
system_schema.keyspaces` with no WHERE returns rows including the
keyspace). The real failure is in `PREPARE` resolution against
`system_schema.*` not exposing column metadata for bind-marker type
inference.

## Observed on

- Ferrosa: 3-node cluster from
  `ferrosa-suite/ferrosa-memory/docker-compose.yml`, 2026-05-11.
- Client: `scylla 0.15.1` (ferrosadb fork rev `2c493e8c`), `LegacySession`.

## Symptom

```rust
session
    .query_unpaged(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?",
        ("agent_memory",),
    )
    .await
// → Database returned an error: The query is syntactically correct but invalid,
//   Error message: PREPARE failed for ... expected 1 bind-marker column spec(s)
//   but resolved only 0.
```

Same query with a literal succeeds:

```rust
session
    .query_unpaged(
        "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = 'agent_memory'",
        (),
    )
    .await
// → Ok(rows: 1)
```

## Expected behavior

`PREPARE` against `system_schema.keyspaces` (and presumably the rest of
`system_schema.*`) MUST resolve the column type for `keyspace_name` (text)
and accept `?` bind markers as it does for any user-keyspace table.

## Misleading error message

Regardless of root-cause priority, the canned suffix `Table
'system_schema.keyspaces' may not yet be visible on this node (schema
replication lag). Retry in a moment.` should not fire when the actual
failure was bind-marker resolution. The retry suggestion sends operators
chasing the wrong issue. Suggest distinguishing "schema not yet visible"
(table missing in metadata) from "bind-marker spec missing" (table
present, type inference failed).

## Workaround in `ferrosa-memory`

`migrate --probe-only` switched to reading the full keyspace list and
filtering client-side instead of using `WHERE keyspace_name = ?`. Cheap
(handful of rows) and avoids the PREPARE failure entirely.
