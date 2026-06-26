---
type: todo
priority: P2
status: draft
created: 2026-05-11
updated: 2026-05-11
---

# Bug: `system_schema.views` row shape breaks scylla-rust-driver auto schema-agreement

## Why this is a Ferrosa bug

Ferrosa advertises Cassandra/Scylla wire-protocol compatibility. The scylla-rust-driver
(both the upstream `scylladb/scylla-rust-driver` and the `ferrosadb/scylla-rust-driver`
fork at rev `2c493e8c`) reads `system_schema.views` during its automatic
schema-agreement metadata fetch — fired after every DDL statement and on
non-Legacy session connects. Ferrosa returns rows whose column shape does not
match what the driver type-checks against, so the driver hard-errors and the
DDL call fails even when the DDL itself was accepted server-side.

This breaks **every Rust client that issues DDL more than once against a Ferrosa
cluster that already has any schema**, including:

- `ferrosa-memory`'s integration tests (cluster-integration CI job).
- The `migrate` binary when re-run against an already-bootstrapped cluster
  (every `CREATE … IF NOT EXISTS` triggers schema-agreement).
- Any consumer trying to apply incremental DDL.

The propagation-barrier failures on `ferrosa-memory` CI (`Schema not visible on
port 19043 within bounded retries`) trace back to this — schema actually
propagates fine (verified independently via the Python `cassandra-driver`,
which reads the same columns without erroring); the rust driver simply can't
decode Ferrosa's response.

## Observed on

- Ferrosa: `ferrosa-memory-node` image built from
  `ferrosa-suite/ferrosa-memory/docker-compose.yml` (current `main`, 2026-05-11),
  3-node cluster + MinIO, auth enabled.
- Client: `scylla 0.15.1` from
  `https://github.com/ferrosadb/scylla-rust-driver?rev=2c493e8cf9968d6c83d33a3dc36c3ef3da595f9a`,
  using `LegacySession` via `SessionBuilder::build_legacy()`.
- Repro: `cargo run --bin migrate -- --contact-points 127.0.0.1:19043
  --keyspace agent_memory --ddl-dir ddl` against a cluster that already has the
  `agent_memory` keyspace.

## Symptom

```
WARN scylla::transport::topology: Failed to fetch metadata using current control connection
  control_connection_address="127.0.0.1:19043"
  error=Cluster metadata fetch error occurred during automatic schema agreement:
  Bad views metadata: system_schema.views has invalid column type:
  TypeCheckError: Failed to type check the Rust type
    (alloc::string::String, alloc::string::String, alloc::string::String)
  against CQL column types [Text, Text, Uuid, Text, Boolean, Boolean, Boolean, Boolean, Uuid, Text]
  : wrong column count: the statement operates on 10 columns, but the given rust types contains 3

Error: apply ddl/001_keyspace.cql: CREATE KEYSPACE IF NOT EXISTS agent_memory

Caused by:
    Cluster metadata fetch error occurred during automatic schema agreement: ...
```

The driver expects a 3-tuple `(keyspace_name text, view_name text, base_table_name text)`
(the Cassandra 3.x reference shape). Ferrosa returns 10 columns including `Uuid`
and `Boolean` fields. The driver's type-check fails strict; even if zero rows
were present the shape mismatch wouldn't trip — but Ferrosa appears to
populate `system_schema.views` from its internal materialized-view registry
even when no user MVs exist (no `CREATE MATERIALIZED VIEW` in the
`ferrosa-memory` DDL bundle).

## Reproducing

```bash
# from ferrosa-suite/ferrosa-memory
docker compose up -d  # 3-node cluster
cargo run --bin migrate -- --contact-points 127.0.0.1:19042 --keyspace agent_memory --ddl-dir ddl
# Succeeds (first run against fresh cluster — system_schema.views is empty here).

cargo run --bin migrate -- --contact-points 127.0.0.1:19043 --keyspace agent_memory --ddl-dir ddl
# Fails with the metadata-fetch error above.

# Python control (succeeds — proves the data is reachable, just not by rust):
python3 -c "
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
c = Cluster(['127.0.0.1'], port=19043,
            auth_provider=PlainTextAuthProvider('ferrosa_admin','ferrosa_admin'))
s = c.connect()
print(list(s.execute(\"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='agent_memory'\")))
"
# [Row(keyspace_name='agent_memory')]
```

## Expected behavior

`system_schema.views` rows MUST conform to one of the documented
Cassandra/Scylla column shapes that mainstream rust/python/java drivers
type-check against. The 3-column shape `(keyspace_name, view_name,
base_table_name)` is the lowest common denominator; the 10-column shape
appears to be a Ferrosa-specific extension and is what the driver chokes on.

Either:

1. Project the user-facing `system_schema.views` view to the Cassandra 3.x
   shape and surface Ferrosa-specific columns under a different system
   table (e.g. `ferrosa_system_schema.views_ext`), or
2. Match the Cassandra 4.x `system_schema.views` shape exactly (which the
   scylla driver supports — `keyspace_name text, view_name text,
   base_table_id uuid, base_table_name text, where_clause text, …`).

Option 2 is the right fix — the driver already knows the 4.x shape.

## Adjacent bug — `WHERE keyspace_name = ?` on `system_schema.keyspaces`

While building a SELECT-only propagation probe in `ferrosa-memory` (see
`bug-system-schema-prepare-rejects-bind-marker.md`), we found that Ferrosa
also rejects `PREPARE 'SELECT keyspace_name FROM system_schema.keyspaces
WHERE keyspace_name = ?'` with `expected 1 bind-marker column spec(s) but
resolved only 0`. Filed separately.

## Real cause confirmed: ferrosa commit `fce7a13`

`fce7a13` ("cql-compat: NoSQLBench gap closure") deliberately extended
`system_schema.views` to the full Cassandra-5.0 column set to satisfy
the DataStax Java Driver 4.x schema parser:

> Gap 7 — system_schema boolean columns. Extend the column metadata
> returned for system_schema.{tables, views, functions} so the driver's
> schema parsers find every Cassandra-5.0 boolean column:
>   - system_schema.views: cdc, include_all_columns, allow_auto_snapshot,
>     incremental_backups (split from the catch-all stub, full Cassandra
>     5.0 column set, 0 rows)

The change is correct for Cassandra-5.0 wire compatibility. The
scylla-rust-driver fork at rev `2c493e8c` predates Cassandra-5.0 and its
`MetadataReader` still type-checks against the 3-column shape — that
fork needs to be bumped to a revision that knows the 5.0 shape, or
patched locally. The Ferrosa side is not wrong; the driver fork is
stale relative to the schema it now serves.

Recommended path: bump `ferrosadb/scylla-rust-driver` to a rev that
matches upstream `scylladb/scylla-rust-driver`'s Cassandra-5.0 metadata
handling (upstream picked up the 5.0 shape some time ago — search
"system_schema.views" in their changelog).

## Workarounds in `ferrosa-memory` (until the driver fork is bumped)

`crates/ferrosa-memory-mcp/src/tools/migrate.rs`:

1. `SessionBuilder::refresh_metadata_on_auto_schema_agreement(false)` —
   keeps the post-DDL schema-agreement wait (cluster convergence) but
   skips the metadata fetch that reads `system_schema.views`. Lets the
   first `CREATE KEYSPACE IF NOT EXISTS` succeed against a fresh
   cluster, which was where the new `fce7a13` regression bit.

2. `--probe-only` mode — SELECT-only path used by the CI cluster
   propagation barrier. Still useful: it costs one cheap query per
   peer instead of replaying 30+ DDL files, and it dodges the related
   bind-marker bug (separate report).

Once the driver fork is bumped, the `refresh_metadata_on_auto_schema_agreement`
override can be removed; `--probe-only` is independently useful and
can stay.
