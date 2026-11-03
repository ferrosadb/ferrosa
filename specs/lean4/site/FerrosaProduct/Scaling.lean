import Ferrosa

/-!
# One binary, every scale: replication and consistency

> *"Start with a single node on your laptop. Add a second node for high availability with automatic
> pair mode. Scale to 3+ nodes and Ferrosa forms a multi-node cluster with Raft consensus, tunable
> consistency levels …"* — ferrosa.ai

Each step up the ladder is a concrete topology in the specification, so its availability and
consistency properties are checked by enumerating every failure pattern rather than argued. The
ladder runs: one node, a pair, three nodes, five nodes, two datacenters over one WAN link, three
datacenters in a ring.
-/

/-!
## The rule every quorum rests on

Two strict majorities of the same replicas always share a replica. That is why a `QUORUM` read sees
a `QUORUM` write: whenever the write and read acknowledgement counts add up to more than the
replication factor, the two sets overlap. No rung of the ladder permits two disjoint write quorums,
and a coordinator never reports success below the level's threshold.
-/

#check @Ferrosa.quorum_intersects
#check @Ferrosa.quorum_wr_intersects
#check @Ferrosa.Topology.Ladder.no_two_disjoint_quorums_at_any_rung
#check @Ferrosa.WritePath.ok_means_threshold_met
#check @Ferrosa.WritePath.short_ack_set_must_fail

/-!
## A single node

Every consistency level is satisfied by the one node, and a read can never miss an acknowledged
write. With the node down nothing is acknowledged: on one node, availability and durability are the
same thing.
-/

#check @Ferrosa.Topology.Single.standalone_read_your_write
#check @Ferrosa.Topology.Single.standalone_down_acks_nothing

/-!
## Pair mode: no split brain

> *"Add a second node and Ferrosa automatically enters pair mode — synchronous replication with
> operator-driven failover paths."* — ferrosa.ai

A pair has no quorum, so it cannot tell a dead peer from a partition. The specification therefore
makes promotion an operator action that requires the peer to be confirmed dead, and proves that in
every reachable state at most one node accepts writes. Without that guard a single promotion splits
the brain. A primary that returns after its peer was promoted rejoins as the secondary, and a pair
whose primary is down and not replaced serves reads but refuses writes until the operator acts.
-/

#check @Ferrosa.Topology.Pair.no_two_writable
#check @Ferrosa.Topology.Pair.promotion_requires_operator_and_dead_peer
#check @Ferrosa.Topology.Pair.unguarded_promotion_splits_brain
#check @Ferrosa.Topology.Pair.returning_primary_becomes_secondary
#check @Ferrosa.Topology.Pair.degraded_pair_stays_read_only
#check @Ferrosa.Topology.Ladder.pair_has_no_quorum

/-!
## Three and five nodes

Three nodes is the first rung with a quorum that can outvote a failed node. At RF = 2 it tolerates no
failure for `QUORUM`: every single node failure blocks some key. Five nodes at RF = 3 survive any one
failure. On every rung `QUORUM` writes with `QUORUM` reads give read-your-writes; `ONE` with `ONE`
does not.
-/

#check @Ferrosa.Topology.Ladder.three_nodes_is_the_first_autonomous_rung
#check @Ferrosa.Topology.ThreeNode.rf2_has_no_fault_tolerance
#check @Ferrosa.Topology.FiveNode.rf3_survives_any_single_failure
#check @Ferrosa.Topology.Ladder.read_your_write_holds_at_every_rung
#check @Ferrosa.Topology.ThreeNode.one_one_is_not_read_your_write
#check @Ferrosa.Topology.Ladder.fault_tolerance_table

/-!
## Consistency levels

The level is configurable per statement, `QUORUM` by default, and a level a client sends is never
overridden by the session default. Each level's acknowledgement count is specified and checked
against an acceptance table. `LOCAL_QUORUM` is computed from the local datacenter's replication
factor; `ONE` offers no read-your-writes.

**Does not hold yet.** For `QUORUM` on a multi-datacenter keyspace the code counts one global majority
where the specification requires a quorum in each datacenter. On `{3,3}` the numbers agree by
accident; on `{2,2}` the code acknowledges without a quorum in the second datacenter.
-/

#check @Ferrosa.Sql.Consistency.default_is_quorum
#check @Ferrosa.Cql.resolveCl_of_some
#check @Ferrosa.Sql.Consistency.acceptance_table_agrees_with_the_rule
#check @Ferrosa.Sql.Consistency.one_read_can_miss_a_one_write
#check @Ferrosa.Cql.code_plan_agrees_only_for_odd_dc_rf
#check @Ferrosa.Topology.ConsistencyLevels.quorum_2x2_undershoots_per_dc_sum
#check @Ferrosa.Sql.Consistency.a_total_count_can_hide_a_missing_datacenter

/-!
## Multiple datacenters

A multi-datacenter keyspace defaults to `DC_QUORUM`: a majority of datacenters *and* a quorum of all
replicas. Any two `DC_QUORUM` writes share a replica in every topology, so no partition can admit two
of them, and it keeps committing through the loss of a whole datacenter. A read in the write's own
datacenter always sees it; a read in another datacenter can miss it.

`LOCAL_QUORUM` exists for Cassandra compatibility. It is strong inside its datacenter and promises
nothing about the others: during a WAN cut both sides can acknowledge writes to the same key on
disjoint replicas, and a failover can lose any number of locally acknowledged writes.

Three datacenters in a ring survive any single link loss, and exactly one datacenter loss.
-/

#check @Ferrosa.Sql.Consistency.the_multi_dc_default_is_dc_quorum
#check @Ferrosa.Topology.DcQuorum.dc_quorum_writes_share_a_replica
#check @Ferrosa.Topology.DcQuorum.no_partition_admits_two_dc_quorum_writes
#check @Ferrosa.Sql.Consistency.dc_quorum_tolerates_a_whole_dc_outage
#check @Ferrosa.Topology.DcQuorum.a_same_origin_read_sees_a_dc_quorum_write
#check @Ferrosa.Topology.DcQuorum.a_read_in_another_datacenter_can_miss_a_dc_quorum_write
#check @Ferrosa.Topology.TwoDc.local_quorum_acks_can_be_disjoint
#check @Ferrosa.Sql.Consistency.a_failover_can_lose_unboundedly_many_local_acks
#check @Ferrosa.Topology.ThreeDcRing.ring_survives_one_link_loss
#check @Ferrosa.Topology.ThreeDcRing.ring_tolerates_exactly_one_dc_loss
