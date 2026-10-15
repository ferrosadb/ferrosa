# After a restart every node reports its own schema version, and drivers never reach schema agreement

**Status**: **partially fixed** — the restart divergence is fixed (a node now adopts the
schema version it persisted instead of the random one minted at boot), and the
controller-built topology/data-movement commands now carry the node's actual
schema version instead of a freshly minted one. Two mint sites remain, each with a
recorded reason (see [Fix](#fix)).
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

## Fix

Two halves, red-first (task t_7dda9421).

### (b) A restarted node adopts the persisted version — landed

The divergence after a restart is `Schema::apply_snapshot`. A snapshot NAMES the
schema state it holds, but `apply_snapshot` merged the contents and dropped the
`version`, so boot step 4a (`ferrosa/src/main.rs`, `schema.apply_snapshot(local_schema_snapshot)`)
left the registry on the random version `SchemaSnapshot::new()` minted when
`Schema::new` constructed it. Every restart therefore rejoined with a fresh
random `system.local.schema_version`, and nothing converged it. `apply_snapshot`
now re-adopts `snapshot.version` last (after the jsonb check, so a refused
snapshot never moves it). Because every restore path goes through
`apply_snapshot`, this also fixes the S3 cold-start bootstrap and the Raft
state-machine's `sync_schema_and_engine_from_state`, which already built a
snapshot carrying `state.schema_version` but had it discarded.

### (a) Operations that change no schema carry the actual version — landed for controller commands

Topology (`JoinNode` / `AssignTokens` / `UpdateNodeInfo`) and data-movement
commands change no schema, so they must carry the node's CURRENT committed
version. The controller's command builders in `controller/membership.rs`
(join, join-refresh, `on_peer_connected`) and `controller/data_movement.rs`
(`ForwardingProposer`, used by decommission and the bootstrap rerun) now read it
from the shared `Schema` instead of calling `Uuid::new_v4()`.

### Deliberately left minting

- `ddl_path.rs:565` (`apply_direct` tail) and `ddl_path.rs:739`
  (`ddl_op_to_raft_command`) — these are the REAL DDL paths. `apply_direct`
  returns early for `JoinNode`, so its tail only runs for a genuine schema
  change, and a new version there is load-bearing: the maintenance loop persists
  `schema.json` only when the version CHANGED (`should_persist_schema(a, b) = a != b`),
  so removing the bump would stop a standalone/pair node from persisting its
  schema at all.
- `data_movement.rs:125` (`impl MembershipProposer for FerrosRaft`) — a bare
  `openraft::Raft` handle holds no schema registry, so it cannot read the node's
  version. Production callers now use `ForwardingProposer`; the impl is left for
  callers that hold only the raft handle. The smallest change that removes the
  mint is a `schema_version: Uuid` parameter on `MembershipProposer::propose`,
  threaded from the caller's `Schema`.

### Why the state machine already agrees

`state_machine.rs` gates the version update on `schema_changed` (topology arms
set it false), so a random version on a topology command never propagated
cluster-wide. The topology contract test
(`topology_command_leaves_the_schema_version_unchanged`) is therefore a
regression guard, not the red that produced the fix; the red is
`apply_snapshot_adopts_the_persisted_schema_version` in `ferrosa-schema`.

### What is now covered by a test

- **Stale persisted version → committed version** —
  `ferrosa-cluster/src/raft/state_machine.rs`,
  `a_stale_persisted_schema_version_converges_to_the_committed_version`. A node
  whose persisted `schema.json` names a STALE version and whose replicated Raft
  state carries the committed one ends on the committed version (`Uuid`
  equality, and asserted NOT equal to either the stale or the boot-minted
  version). It drives the real boot-order functions — `Schema::apply_snapshot`
  for boot step 4a, then `recover_from_persisted_snapshot` →
  `sync_schema_and_engine_from_state` — so it fails if either leg stops doing its
  job. **CONTROL:** with the version adoption in `apply_snapshot` disabled the
  test fails at RUN TIME on the convergence assertion (`left` = the boot-minted
  version, `right` = the committed version). It has no red of its own — the
  adoption is already in the tree — so the CONTROL is what proves the test
  exercises the mechanism.
- `ferrosa-schema/src/registry.rs`,
  `apply_snapshot_adopts_the_persisted_schema_version` — the registry-level red
  that produced the fix.
- `ferrosa-cluster/src/raft/state_machine.rs`,
  `real_ddl_command_changes_the_schema_version` — the `apply` (log-replay) path
  still moves the registry to the version a committed command carries.

**Not covered:** a real crash, and the case where the node's persisted Raft
snapshot PREDATES the DDL. Then only the commit log carries the committed
version and convergence depends on re-applying the DDL entry through `apply`,
not on `recover_from_persisted_snapshot`.

## Known window (observed, not fixed)

A restarted node can REPORT a stale `system.local.schema_version` before the Raft
state-machine recovery runs. Boot step 4a lands the persisted `schema.json`
version in the registry early (`ferrosa/src/main.rs`), while the step that
re-adopts the committed version (`recover_from_persisted_snapshot` →
`sync_schema_and_engine_from_state`) runs later, inside `transition_to_cluster`
(`ferrosa-cluster/src/controller/cluster.rs`), which is gated on cluster
formation. Between the two, `Schema::snapshot().version` — the value
`system.local` reports (`ferrosa-schema/src/system/local.rs`) — is the STALE one.
The convergence test asserts that intermediate state
(`version_after_persisted_restore == stale_version`), which is the window; a read
during it on a live cluster was not measured.

## Not acceptable as a fix

- Making clients skip schema agreement, or treat a timeout as success. Agreement is how a client
  knows its DDL is visible everywhere.
- A periodic no-op DDL to "nudge" versions together. That is the operator's emergency remedy
  (documented in the ferrosa-enterprise runbook), not a fix.
