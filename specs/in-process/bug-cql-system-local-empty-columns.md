---
type: bug
priority: P1
status: open
created: 2026-05-10
discovered-by: ferrosa-jepsen test cql_session::tests::rust_driver_connects_to_cluster
---

# Bug: `SELECT now() FROM system.local` returns row with zero columns

## Symptom

```sql
SELECT now() FROM system.local
```

returns a row with zero columns. Multiple Cassandra-compatible drivers crash:

- **cdrs-tokio** (used by `ferrosa-jepsen/src/cql_session.rs`):

  ```
  thread '...' panicked at ferrosa-jepsen/src/cql_session.rs:133:40:
  index out of bounds: the len is 0 but the index is 0
  ```

  (`rows[0]` returned but `rows[0][0]` indexes an empty Vec.)

- **python `cassandra-driver`** v3.x:

  ```
  ValueError: Invalid shape in axis 0: 0.
    File "cassandra/deserializers.pyx", line 515, in cassandra.deserializers.obj_array
    File "<stringsource>", line 159, in View.MemoryView.array.__cinit__
  ```

Both drivers receive the row but the column count in the response is 0,
which is malformed per the CQL native protocol spec.

## Reproduction

1. Bring up the jepsen cluster:
   ```sh
   cd /home/bkearns/src/ferrosa-suite/feature-raft-gap-close
   docker compose -f ferrosa-jepsen/tests/docker/jepsen-cluster.yml up -d
   ```
   Wait for `raft leader elected` in the logs (10–20 s typical).

2. Run the test (FAILS):
   ```sh
   FERROSA_TEST_CONTAINERS=1 cargo test -p ferrosa-jepsen --lib \
     cql_session::tests::rust_driver_connects_to_cluster -- --nocapture
   ```

3. Or via Python:
   ```sh
   docker run --rm --network host python:3.12-slim sh -c '
     pip install -q cassandra-driver
     python -c "from cassandra.cluster import Cluster
   c = Cluster([\"localhost\"], port=49042)
   s = c.connect()
   print(list(s.execute(\"SELECT now() FROM system.local\")))"
   '
   ```

## Where it surfaces

`ferrosa-jepsen/src/cql_session.rs:122-138` — the test connects via
`ScyllaCqlSession::connect()` and then runs `SELECT now() FROM
system.local`. The connect itself succeeds, the query response arrives,
but the row carries zero columns.

## What's NOT broken

- The **driver** test (`ferrosa-jepsen/src/driver/rust_driver.rs::tests::rust_driver_connects_to_cluster`)
  passes on the same cluster — that test exercises a different code path
  (driver harness for `register` workload). So the CQL native protocol
  basic connect + auth + handshake works.

- All 6 unit-level `docker_provision::tests` pass; cluster bring-up + tear-down
  via `provision_docker_cluster` works.

## What's unknown

- **Regression vs latent**: this is the first time the test has actually
  been able to run against a real cluster — it was previously gated by
  `FERROSA_TEST_CONTAINERS` and panicked immediately. Cannot say whether
  the bug is recent (Sprint 1–8 introduced) or pre-existing.

- **Scope**: only `SELECT now() FROM system.local` confirmed broken.
  Other queries against `system.local` (e.g. `SELECT release_version, cluster_name`)
  — the Python probe returned the same error on the first query, so the
  `system.local` table itself may be the culprit. Or it may be that
  empty rowsets in general have wrong column counts.

## Suspected root cause

Either:

1. **`system.local` virtual table returns no columns** when projected with
   a function (`now()`). The function projection isn't generating the
   expected column metadata in the result frame.

2. **Empty/single-row queries on virtual tables** drop their column
   metadata in the response encoder somewhere in `ferrosa-cql`.

## Next steps

1. Run other queries against `system.local` to narrow whether it's the
   `now()` function specifically or any virtual-table SELECT:
   - `SELECT release_version FROM system.local` (column from the row directly)
   - `SELECT 1 FROM system.local` (literal projection)
   - `SELECT now() FROM system_schema.tables LIMIT 1` (now() against a real table)

2. Walk `ferrosa-cql/src/result.rs` (or wherever rows are encoded into
   the wire format) for the path that handles function projections and
   virtual-table sources. Look for a missing column-metadata emit on the
   empty/single-row path.

3. Check git blame for recent edits to `system.local` virtual-table
   handling in any of Sprint 1–8 commits. Sprint 6 (multi-DC) and
   Sprint 8 (learners) both touch `state.rs` which may interact with
   `system.peers`/`system.local`.

## Acceptance

- The reproduction in §"Reproduction" above returns a row with at least
  one column carrying the `now()` timeuuid.
- `cql_session::tests::rust_driver_connects_to_cluster` passes when run
  with `FERROSA_TEST_CONTAINERS=1` against the docker compose cluster on
  4xxxx ports.
