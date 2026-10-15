# After a restart every node reports its own schema version, and drivers never reach schema agreement

**Status**: reported, not fixed
**Found**: 2026-10-15, ferrosa-ent (AWS, 3 nodes, v2026.10.15.0002; the 2026-10-08 image binary behaves the same)
**Severity**: outage for any client that waits for schema agreement. ferrosa-dbaas `dbaas-api` cannot
start after any database node restarts: its startup migration (`CREATE ... IF NOT EXISTS`) waits for
agreement that never comes, and the service crash-loops. It took the enterprise console's API down
for about 20 minutes.

## What happens

1. A node restarts. It comes back with a fresh random `system.local.schema_version`
   (`Uuid::new_v4()`; see `ferrosa-cluster/src/controller/*`, `ddl_path.rs`).
2. Its `system.peers` rows report **its own** version for every peer
   (`ferrosa-schema/src/system/peers.rs` writes `peer.schema_version = snap.version`;
   `controller/mod.rs` documents this). So a check that reads only one node always agrees with itself.
3. A driver that compares `system.local.schema_version` across all its connections (the Rust driver
   ferrosa-dbaas uses does this) sees N different values and waits until its timeout.
4. Nothing converges the versions over time. Watched for 3 minutes after a rolling restart: no change.
5. The next real DDL that goes through Raft gives every node the leader's version, and agreement
   returns at once (the propagation from `0448b6f8`). `IF NOT EXISTS` statements that change nothing
   do not count.

So after any restart (a release roll, a host reboot, a crash) a cluster disagrees with itself until
someone happens to run DDL.

## Reproduction (3-node cluster)

```python
# as any superuser, one session per node (whitelist policy), protocol v4
for node in nodes:
    print(node, session_on(node).execute("SELECT schema_version FROM system.local").one())
# -> one value on all three (after some DDL)

# restart ONE node (systemctl restart ferrosa-engine), wait for /readyz
# -> that node now prints a different value; peers on each node still echo that node's own value

# CREATE TABLE ks.probe (k int PRIMARY KEY); DROP TABLE ks.probe;
# -> all three print the same value again
```

Observed on ferrosa-ent 2026-10-15 05:13–05:35 UTC: three distinct versions after the roll to
v2026.10.15.0002 (03:53 UTC). dbaas-api had last started at 03:42, before the roll; every start
after it failed with
`control schema migration failed: ... statement failed (migration v0): CREATE TABLE IF NOT EXISTS control.schema_version`.
One create+drop in a scratch keyspace converged all three, and dbaas-api started in 15 s.

## Expected

A node that restarts or rejoins adopts the cluster's current schema version (the version committed
with the last applied schema change), not a random one. Versions should differ only while a schema
change is actually being applied, and a node's peer rows should report what each peer actually holds.

## Not acceptable as a fix

- Making clients skip schema agreement, or treat a timeout as success. Agreement is how a client
  knows its DDL is visible everywhere.
- A periodic no-op DDL to "nudge" versions together. That is the operator's emergency remedy
  (documented in the ferrosa-enterprise runbook), not a fix.
