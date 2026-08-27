# RF=3 formation must not degrade silently to pair mode

**Severity:** P0 durability and availability
**Forge tasks:** `t_9bd0c95c`, `t_891840e7`, `t_05acdc8a`
**Last revised:** 2026-08-26

## Failure

The seedless bootstrapper accepted a transport callback carrying its own host
ID. One real peer plus self satisfied the two-peer threshold, so that node
entered Raft formation while the two spokes remained independent pairs. A pair
primary could advertise CQL readiness even though the deployment was intended
to contain three Raft voters.

## Contract

- Inbound and outbound self-connections are rejected before they mutate peer
  tracking, Raft routing, or deployment mode.
- Operators set `FERROSA_EXPECTED_CLUSTER_SIZE=3` for the three-node native
  cluster. An explicit size fails readiness closed:
  - `1` requires standalone mode;
  - `2` requires pair-primary mode;
  - `3+` requires cluster mode, a current Raft leader, and at least that many
    committed voters.
- Automatic standalone/pair behavior remains available only when the variable
  is unset.
- Formation DDL buffering is a fixed-capacity queue. Saturation returns a
  retriable error immediately instead of growing memory.
- Cluster-member marker recovery reads at most 4 KiB and restores through
  stage, fsync, length verification, and atomic rename.

## Regression evidence

- `self_connection_does_not_advance_cluster_formation`
- `configured_three_node_pair_primary_is_not_cql_ready`
- `configured_cluster_readiness_requires_declared_committed_voters`
- `forming_ddl_path_fails_fast_when_bounded_queue_is_full`
- `ddl_during_forming_queues_and_replays`
- `resetting_a_stranded_node_rejects_an_oversized_membership_marker`

The changed production files must report zero findings from
`frg materialization-scan`.
