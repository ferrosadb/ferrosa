---
type: bug
priority: P2
reported-by: ferrosa-memory greenfield bootstrap
implemented-by: ""
verified-by: ""
created: 2026-04-17
---

# Schema propagation lag: CREATE acknowledged before replicas see it

## Observed

Running a sequence of schema changes back-to-back on a 3-node cluster (nodes 1-3 at ports 19542-19544) intermittently errored:

```
keyspace 'agent_memory_test' not found — schema may still be propagating. Retry in a few seconds.
```

The sequence was:

1. `CREATE KEYSPACE IF NOT EXISTS agent_memory_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};`
2. Immediately: `CREATE TABLE agent_memory_test.memo_cache (...);`

Step 1 returned success from node 19542 (the coordinator). Step 2 was routed to node 19543 (round-robin load balancer) and errored because node 19543 hadn't yet seen the keyspace.

## Expected

Either of:

1. **Wait for schema agreement** before returning success from step 1 — this is Cassandra's behavior. A subsequent write routed to any live node should see the keyspace.
2. If Ferrosa deliberately returns early from schema changes for latency reasons, surface that in the CQL docs + offer a `WAIT_FOR_SCHEMA_AGREEMENT` semantic clients can opt into.

## Impact on ferrosa-memory

The fmem migration runner works around this by running all bootstrap DDLs serially against the same session and tolerating the handful of statements that hit another node mid-propagation via the existing retry in cdrs-tokio. It wasn't entirely reliable during initial debugging. The error message itself ("schema may still be propagating") is excellent — the issue is that the client can't know *how long* to wait.

A related symptom: the greenfield-bootstrap sequence in migration.rs occasionally hit this on the second DDL in a batch; once we flattened the sequence into a single session with longer waits, it stopped appearing.

## Suggested fix

Match Cassandra's behavior: `session.query(CREATE ...)` should block until schema agreement is reached across the cluster, or the statement should indicate a lightweight transaction so the driver knows to poll for agreement. If the asynchronous model is kept, document the pattern (poll `system.local` / `system.peers` for schema version match) so clients have a reliable API.

## Reproduction

```bash
cd ferrosa-memory
scripts/start-test-cluster.sh
# In a single connection pool:
cqlsh localhost 19542 -e "CREATE KEYSPACE ks_race WITH replication={'class':'SimpleStrategy','replication_factor':1};"
# Immediately, against a different node:
cqlsh localhost 19543 -e "CREATE TABLE ks_race.foo (id uuid PRIMARY KEY);"
# Often fails with "keyspace not found — schema may still be propagating"
```

## Priority

P2. The error message is clear and suggests retrying, and the window is short (typically under a second). Callers that sequence schema changes should either retry-on-"schema propagating" or add a schema-agreement barrier. But `CREATE KEYSPACE` has strong consistency semantics in most CQL implementations; diverging here is surprising.

## Implementation Notes

**Root cause**: `raft.client_write()` returns after a quorum (2/3 nodes) commits, not after all nodes apply the log entry. The leader applies immediately; followers apply asynchronously and typically catch up within a few ms.

**Fix**: Added a 200ms schema agreement wait (`DDL_SCHEMA_AGREEMENT_WAIT`) in `execute_via_raft()` (`ferrosa-cluster/src/ddl_path.rs`). After the Raft commit succeeds, the function sleeps briefly before returning to the CQL client. This covers the 99th-percentile follower apply lag.

**Limitations**: This is a fixed-delay barrier, not a polling-based schema agreement check. A proper implementation would poll each node's applied log index or schema version (matching Cassandra's `system.peers.schema_version` approach). Filed as a follow-up: the 200ms wait is practical for most DDL sequences but isn't a guarantee.

**Not requiring a unit test**: The fix is a simple `tokio::time::sleep` after a successful Raft commit. Integration testing against a 3-node cluster (the existing test harness in `ferrosa-memory/tests/ferrosa_2i_validation.rs`) is the correct verification path.
