# After a restart every node reports its own schema version, and drivers never reach schema agreement

**Status**: **fixed** — a restarted node adopts the schema version it persisted
instead of the random one minted at boot, and every `RaftCommand` built for an
operation that changes NO schema now carries the node's ACTUAL current committed
version instead of `Uuid::new_v4()`. Two mint sites remain, both on the REAL DDL
paths where a new version is correct; each has a recorded reason (see
[Fix](#fix)). Sweep task: t_17569632 (parent fix t_7dda9421).
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

### (a) Operations that change no schema carry the actual version — swept

Topology (`JoinNode` / `AssignTokens` / `UpdateNodeInfo` / `SetNodeState` /
`LeaveNode` / `ApproveNode`) and data-movement (`RecordBootstrapComplete`) ops
change no schema, so a command naming one must carry the node's CURRENT
committed version. Every construction site that minted `Uuid::new_v4()` for such
an op is now gone. What was swept (task t_17569632):

- `controller/membership.rs` — join, join-refresh and `on_peer_connected` read
  `self.schema.snapshot().version` (landed with the parent fix).
- `controller/cluster.rs` — the eight commands the cluster-formation task builds
  (`UpdateNodeInfo` / `AssignTokens` on the recovered-topology refresh, `JoinNode`
  and `AssignTokens` for the seed and for each peer, `SetNodeState` on the promote
  pass). The task reads the version ONCE per spawn through
  `controller::cluster::current_schema_version(&schema_for_bootstrap)` and reuses
  it for every command, so the per-node loops do not re-read the snapshot.
- `controller/data_movement.rs` — `MembershipProposer::propose` now takes a
  `schema_version: Uuid` parameter, so the impls no longer choose the version
  themselves: `impl MembershipProposer for FerrosRaft` uses the parameter (its
  mint is what the parent fix left behind), and `ForwardingProposer` no longer
  carries the version in a field. `decommission_verified` and
  `bootstrap_verified` take the version and thread it from the caller's
  `Schema` (`initiate_decommission`, `rerun_local_bootstrap`).
- `membership/mod.rs` — `MembershipChanger`'s seven commands (`AccordApply`,
  `JoinNode`, `SetNodeState` ×2, `LeaveNode`, `ApproveNode`,
  `UpdateNodeInfo`). The changer holds no registry, so it carries a
  `schema_version` field set by the new `with_schema_version` builder; see the
  note below for the default.
- `rebalance.rs` — `execute_rebalance`'s `AssignTokens` stamps the version it
  already holds in its `schema` argument.
- `ferrosa/src/web/api.rs` — `add_node_handler`'s `ApproveNode` stamps
  `ModeController::schema().snapshot().version`.
- `raft_forward.rs` — checked, nothing to sweep: the module only DECODES and
  forwards an already-built `RaftCommand`; it constructs none outside its
  `#[cfg(test)]` fixtures.

### Deliberately left minting

- `ddl_path.rs:565` (`apply_direct` tail) and `ddl_path.rs:739`
  (`ddl_op_to_raft_command`) — these are the REAL DDL paths. `apply_direct`
  returns early for `JoinNode`, so its tail only runs for a genuine schema
  change, and a new version there is load-bearing: the maintenance loop persists
  `schema.json` only when the version CHANGED (`should_persist_schema(a, b) = a != b`),
  so removing the bump would stop a standalone/pair node from persisting its
  schema at all. `ddl_op_to_raft_command` names the new version a DDL is about to
  commit; minting it is the point.
- `MembershipChanger`'s DEFAULT version is `Uuid::nil()`, not a mint. The changer
  cannot reach a registry, and every op it sends is gated off by the state
  machine's `schema_changed`, so nil is an inert, deterministic "not known here":
  it can never churn a version. Callers that hold the node's `Schema` MUST set the
  real one with `with_schema_version`. Nil is a recorded choice, not an oversight.
- Random versions inside `#[cfg(test)]` modules and `tests/` are fixtures for
  test-built commands, not production mints.

### Why the state machine already agrees

`state_machine.rs` gates the version update on `schema_changed` (topology arms
set it false), so a random version on a topology command never propagated
cluster-wide. The topology contract test
(`topology_command_leaves_the_schema_version_unchanged`) is therefore a
regression guard, not the red that produced the fix; the red that produced the
restart half is `apply_snapshot_adopts_the_persisted_schema_version` in
`ferrosa-schema`.

For this sweep the red is at COMMAND CONSTRUCTION, not at the state machine: a
state-machine-level test passes with or without the sweep (it did — the gate
predates it). `data_movement_commands_carry_the_current_schema_version`
(`controller::data_movement`) drives the real `decommission_verified` /
`bootstrap_verified` path with a recording proposer and asserts the VALUE of the
command's `schema_version` — it did not compile while `propose` took no version,
and it fails at run time if any mint is restored on that path (verified by
restoring one). The tests added next to code that already existed
(`topology_commands_carry_the_current_schema_version`,
`membership_commands_carry_the_current_schema_version`) are labelled regression
guards in their own doc comments: those two sites sit behind a live
`openraft::Raft` or inside a spawned task and have no unit-level construction
seam.

## Not acceptable as a fix

- Making clients skip schema agreement, or treat a timeout as success. Agreement is how a client
  knows its DDL is visible everywhere.
- A periodic no-op DDL to "nudge" versions together. That is the operator's emergency remedy
  (documented in the ferrosa-enterprise runbook), not a fix.
