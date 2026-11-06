/-!
# Register II: Consistency, replication and topology

What you are allowed to assume once the cluster is more than one node. The escalation ladder, one rung per module: a single node, a pair, three at RF=2, five at RF=quorum, two datacenters over one WAN link, three in a ring. Each rung is a concrete `Topo`, so its quorum properties are finite and checked by enumeration rather than argued. The Accord modules sit above them and specify the apply order and the recovery path a client actually observes. The last two modules come back down to a single node's apply path: what a client may assume once raft reports a schema change applied, and the two places that assumption stops.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Topology.Single`

[The module page](Ferrosa/Topology/Single/)

- [`standalone_ok_at_every_level`](Ferrosa/Topology/Single/#Ferrosa___Topology___Single___standalone_ok_at_every_level): Every CL is satisfied by the single node — `QUORUM`, `ALL`, `LOCAL_QUORUM` all resolve to "the node is up".
- [`standalone_down_acks_nothing`](Ferrosa/Topology/Single/#Ferrosa___Topology___Single___standalone_down_acks_nothing): With the node down nothing is acked — there is no second copy to fall back to, so availability and durability are the same thing.
- [`standalone_read_your_write`](Ferrosa/Topology/Single/#Ferrosa___Topology___Single___standalone_read_your_write): A read can never miss an acked write: the ack set and the probe are the same single node.

## `Ferrosa.Topology.Pair`

[The module page](Ferrosa/Topology/Pair/)

- [`t1_roles_are_determined`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___t1_roles_are_determined): T1 assigns both roles at once: the receiver is primary, the initiator is secondary.
- [`promotion_requires_operator_and_dead_peer`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___promotion_requires_operator_and_dead_peer): Promotion is operator-only, and requires the peer confirmed dead.
- [`no_two_writable`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___no_two_writable): No split brain.
- [`unguarded_promotion_splits_brain`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___unguarded_promotion_splits_brain): **Counterexample.** Counterexample: without the guard, one promotion splits the brain.
- [`promote_after_primary_failure_reachable`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___promote_after_primary_failure_reachable): The promotion path is reachable in the guarded system: fail the primary, promote the secondary.
- [`returning_primary_becomes_secondary`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___returning_primary_becomes_secondary): A returning primary is demoted by connection direction.
- [`degraded_pair_is_read_only`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___degraded_pair_is_read_only): Degraded pair serves stale reads only.
- [`degraded_pair_stays_read_only`](Ferrosa/Topology/Pair/#Ferrosa___Topology___Pair___degraded_pair_stays_read_only): ...

## `Ferrosa.Topology.ThreeNode`

[The module page](Ferrosa/Topology/ThreeNode/)

- [`rf2_quorum_is_both_replicas`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___rf2_quorum_is_both_replicas): `QUORUM` at RF = 2 means both replicas.
- [`one_replica_down_blocks_quorum_write`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___one_replica_down_blocks_quorum_write): With one replica of the key down, `QUORUM` writes are refused.
- [`rf2_has_no_fault_tolerance`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___rf2_has_no_fault_tolerance): The rung has no fault tolerance.
- [`quorum_quorum_is_read_your_write`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___quorum_quorum_is_read_your_write): `QUORUM`/`QUORUM` is read-your-write at RF = 2: both operations must reach the same two replicas, and `W + R = 4 > 2`.
- [`one_one_is_not_read_your_write`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___one_one_is_not_read_your_write): `ONE`/`ONE` is not read-your-write at RF = 2: a write acked by one replica can be missed by a read served by the other.
- [`all_equals_quorum_at_rf2`](Ferrosa/Topology/ThreeNode/#Ferrosa___Topology___ThreeNode___all_equals_quorum_at_rf2): `ALL` degrades to `QUORUM` at RF = 2 — no durability gain, only stricter availability loss.

## `Ferrosa.Topology.FiveNode`

[The module page](Ferrosa/Topology/FiveNode/)

- [`rf3_quorum_is_two`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___rf3_quorum_is_two): `QUORUM` at RF = 3 is two acks — exactly one replica of slack.
- [`rf3_survives_any_single_failure`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___rf3_survives_any_single_failure): One node may fail without losing any key.
- [`rf3_blocked_by_two_replica_failures`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___rf3_blocked_by_two_replica_failures): Two simultaneous replica failures do block some key, at RF = 3.
- [`rf3_quorum_quorum_is_read_your_write`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___rf3_quorum_quorum_is_read_your_write): `QUORUM`/`QUORUM` is read-your-write at RF = 3 (`2 + 2 > 3`).
- [`cluster_majority_read_can_miss_replica_write`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___cluster_majority_read_can_miss_replica_write): A cluster-majority read (3 of 5) can miss a replica-quorum write (2 of 3).
- [`quorum_is_over_replicas_not_cluster`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___quorum_is_over_replicas_not_cluster): The two quorum bases genuinely differ at this rung: 2 of 3 replicas is not a majority of the 5-node cluster (`2 * 2 < 5`).
- [`quorum_and_cluster_majority_differ`](Ferrosa/Topology/FiveNode/#Ferrosa___Topology___FiveNode___quorum_and_cluster_majority_differ): Which is why a `SERIAL`/leader read is the only way to see the newest write at this rung without reading the quorum's own replica set.

## `Ferrosa.Topology.TwoDc`

[The module page](Ferrosa/Topology/TwoDc/)

- [`nts_local_threshold_is_two`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___nts_local_threshold_is_two): Per-DC RF is 2, so LOCAL_QUORUM is 2 acks — both local replicas.
- [`local_quorum_survives_wan_cut`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___local_quorum_survives_wan_cut): DC0 keeps LOCAL_QUORUM availability through the WAN cut.
- [`local_quorum_survives_wan_cut_dc1`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___local_quorum_survives_wan_cut_dc1): DC1 keeps it too, from its own side of the cut.
- [`global_quorum_blocked_by_wan_cut`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___global_quorum_blocked_by_wan_cut): `QUORUM` over RF = 4 needs 3 acks; the cut leaves 2.
- [`each_quorum_blocked_by_wan_cut`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___each_quorum_blocked_by_wan_cut): `EACH_QUORUM` requires a local quorum in every DC, so one DC down blocks it even though the surviving DC is healthy.
- [`local_quorum_acks_can_be_disjoint`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___local_quorum_acks_can_be_disjoint): LOCAL_QUORUM acks can be disjoint.
- [`global_rf_local_quorum_would_over_ask`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___global_rf_local_quorum_would_over_ask): Feeding the global RF to a LOCAL level is a category error.
- [`local_quorum_threshold_depends_on_rf_used`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___local_quorum_threshold_depends_on_rf_used): The two numbers in one statement: what the global-RF path would ask for, versus what the local DC can supply.
- [`local_one_global_path_is_not_local`](Ferrosa/Topology/TwoDc/#Ferrosa___Topology___TwoDc___local_one_global_path_is_not_local): `LOCAL_ONE` computed on the global path is not local at all: it will ack on a remote replica, which is what `cl_routing` then filters away for the voter-quorum levels.

## `Ferrosa.Topology.ThreeDcRing`

[The module page](Ferrosa/Topology/ThreeDcRing/)

- [`ring_survives_one_link_loss`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___ring_survives_one_link_loss): Any single link loss leaves the ring connected.
- [`ring_survives_one_loss_fully`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___ring_survives_one_loss_fully): After any one link loss the ring is still fully connected — that is what a ring buys over a star.
- [`two_link_loss_isolates_at_most_one_dc`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___two_link_loss_isolates_at_most_one_dc): Losing two of the three links isolates one DC, but some cross-DC path always survives — the two remaining DCs can still talk.
- [`ring_disconnects_when_fully_cut`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___ring_disconnects_when_fully_cut): Losing all three links disconnects the DCs.
- [`isolating_one_dc_keeps_the_other_two_connected`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___isolating_one_dc_keeps_the_other_two_connected): Isolating DC2 leaves DC0 and DC1 connected to each other.
- [`rf6_quorum_is_four`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___rf6_quorum_is_four): `QUORUM` at RF = 6 is four acks.
- [`quorum_of_six_spans_two_dcs`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___quorum_of_six_spans_two_dcs): No single DC can carry a cross-DC quorum: a DC holds 3 replicas but 4 are needed, so every quorum spans at least two DCs.
- [`single_dc_loss_keeps_quorum_for_every_key`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___single_dc_loss_keeps_quorum_for_every_key): A single DC loss keeps `QUORUM` for every key.
- [`two_dc_loss_breaks_quorum`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___two_dc_loss_breaks_quorum): Two DC losses do not.
- [`each_quorum_never_survives_dc_loss`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___each_quorum_never_survives_dc_loss): `EACH_QUORUM` never survives an isolated DC: it demands a local quorum in the DC that is gone.
- [`ring_tolerates_exactly_one_dc_loss`](Ferrosa/Topology/ThreeDcRing/#Ferrosa___Topology___ThreeDcRing___ring_tolerates_exactly_one_dc_loss): The ring tolerates exactly one DC loss: one is free, two take `QUORUM` with them.

## `Ferrosa.Topology.Ladder`

[The module page](Ferrosa/Topology/Ladder/)

- [`quorum_threshold_table`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___quorum_threshold_table): The threshold table.
- [`fault_tolerance_table`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___fault_tolerance_table): Fault tolerance per rung.
- [`escalation_thresholds_monotone`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___escalation_thresholds_monotone): Thresholds never decrease as the ladder climbs.
- [`read_your_write_holds_at_every_rung`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___read_your_write_holds_at_every_rung): Read-your-write survives every rung: at each one, `QUORUM`+`QUORUM` satisfies `W + R > RF`.
- [`pair_has_no_quorum`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___pair_has_no_quorum): Pair mode has no quorum at all.
- [`three_nodes_is_the_first_autonomous_rung`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___three_nodes_is_the_first_autonomous_rung): Three nodes is the first rung with an autonomous quorum.
- [`quorum_share_never_grows`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___quorum_share_never_grows): Escalation never widens a quorum's *share* of the replica set: the ack threshold grows more slowly than the RF.
- [`no_two_disjoint_quorums_at_any_rung`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___no_two_disjoint_quorums_at_any_rung): No rung permits two disjoint write quorums.
- [`local_quorum_needs_at_least_two_dcs`](Ferrosa/Topology/Ladder/#Ferrosa___Topology___Ladder___local_quorum_needs_at_least_two_dcs): The two-DC rung is the first that can serve `LOCAL_QUORUM` writes on both sides of a cut, which is also the first rung that can accept two divergent writes for the same key (`TwoDc.local_quorum_acks_can_be_disjoint`).

## `Ferrosa.Topology.DcQuorum`

[The module page](Ferrosa/Topology/DcQuorum/)

- [`dcMajority_count_iff`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dcMajority_count_iff): `count > rf / 2` is exactly a strict majority of the datacenter's universe.
- [`dcMajority_pos`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dcMajority_pos): A datacenter's majority is never zero acknowledgements.
- [`dc_gate_forces_a_shared_datacenter`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dc_gate_forces_a_shared_datacenter): Two writes that pass the datacenter gate share a datacenter where both hold a majority.
- [`dc_gate_forces_a_shared_replica`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dc_gate_forces_a_shared_replica): Gate 2 alone forces a shared replica, for every topology and every pair of origins.
- [`dc_quorum_writes_share_a_replica`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dc_quorum_writes_share_a_replica): Any two DC_QUORUM writes share a replica, whatever their origins.
- [`no_partition_admits_two_dc_quorum_writes`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___no_partition_admits_two_dc_quorum_writes): No partition admits two DC_QUORUM writes.
- [`sum_count_le`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___sum_count_le): The counting lemma, summed over any list of datacenters.
- [`exists_pos_of_sum_pos`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___exists_pos_of_sum_pos): A positive sum has a positive term.
- [`node_gate_forces_a_shared_replica`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___node_gate_forces_a_shared_replica): Gate 3 alone forces a shared replica.
- [`dc_quorum_write_is_a_cassandra_quorum_write`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dc_quorum_write_is_a_cassandra_quorum_write): A DC_QUORUM write is a Cassandra QUORUM write: gate 3 is Cassandra's rule.
- [`dc_quorum_and_cassandra_quorum_writes_share_a_replica`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___dc_quorum_and_cassandra_quorum_writes_share_a_replica): Mixed levels still intersect.
- [`a_local_read_sees_every_write_that_reached_its_datacenter`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_local_read_sees_every_write_that_reached_its_datacenter): A local read sees every write that reached a majority of the reader's datacenter.
- [`a_same_origin_read_sees_a_dc_quorum_write`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_same_origin_read_sees_a_dc_quorum_write): A read in the write's origin datacenter always sees it, for every topology.
- [`a_read_in_another_datacenter_can_miss_a_dc_quorum_write`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_read_in_another_datacenter_can_miss_a_dc_quorum_write): A read in another datacenter can miss a DC_QUORUM write.
- [`filter_length_mono`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___filter_length_mono): A weaker filter keeps at least as many elements.
- [`votes_length_mono`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___votes_length_mono): A datacenter that has reached its own majority keeps it as more acknowledgements arrive, so the number of datacenters that vote never falls.
- [`write_is_monotone`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___write_is_monotone): More acknowledgements never revoke a write.
- [`a_dark_majority_of_datacenters_blocks_every_write`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_dark_majority_of_datacenters_blocks_every_write): A dark majority of datacenters blocks every write.
- [`rfOf_ge`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___rfOf_ge): A datacenter index past the end of the configured list has replication factor zero, which is the arithmetic behind "an unconfigured datacenter holds no replicas".
- [`rfOf_append_zero`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___rfOf_append_zero): Appending a datacenter with no replicas leaves every replication factor where it was — its own, necessarily, and every existing one too.
- [`eligibleDcs_append_zero`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___eligibleDcs_append_zero): A datacenter with no replicas is not counted, so the denominator gate 2 measures against is the same with or without it.
- [`rfTotal_append_zero`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___rfTotal_append_zero): The replica total gate 3 measures against is unchanged as well, so a datacenter added with no replicas cannot lower the bar for anyone else's write.
- [`a_zero_rf_datacenter_is_inert`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_zero_rf_datacenter_is_inert): Configuring a datacenter with no replicas changes no verdict, which is what the code's `rf > 0` filters are for: it neither raises the datacenter denominator nor the replica total.
- [`a_zero_rf_origin_fails_closed`](Ferrosa/Topology/DcQuorum/#Ferrosa___Topology___DcQuorum___a_zero_rf_origin_fails_closed): And the origin gate fails closed on an `rf = 0` origin: no acknowledgement count passes it.

## `Ferrosa.Topology.ConsistencyLevels`

[The module page](Ferrosa/Topology/ConsistencyLevels/)

- [`ack_rule_is_topological`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___ack_rule_is_topological): The rule reads only `(acks, cl, rf)`.
- [`one_ack_suffices_at_rf_one`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___one_ack_suffices_at_rf_one): `RF = 1` acknowledges on the first ack, at every level.
- [`quorum_rf3_needs_two`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_rf3_needs_two): `RF = 3` at `QUORUM` needs two acks — the counting argument that gives `W + R > RF` with a matching read.
- [`local_quorum_rf3_needs_two`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___local_quorum_rf3_needs_two): `LOCAL_QUORUM` in a three-replica DC needs two *local* acks.
- [`txnAckOk_iff`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___txnAckOk_iff): A transaction is acknowledged exactly when every key it wrote has met its own level, spelled out as a `∀` over the keys — which is all the composition there is.
- [`txnAckOk_nil`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___txnAckOk_nil): A transaction that wrote nothing is trivially acknowledged; one that wrote a single key reduces to that key's rule.
- [`txnAckOk_single`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___txnAckOk_single): A one-key transaction's verdict is that key's ack rule and nothing more, so a single-key transaction never waits on a second key's replicas.
- [`localQuorumThreshold_eq`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___localQuorumThreshold_eq): Stated as an equation: the threshold is `localRf / 2 + 1`, whatever any other DC's replica count happens to be.
- [`global_rf_would_over_ask_2_2`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___global_rf_would_over_ask_2_2): Feeding `blockFor` the *global* RF instead gives a different answer, which is why the two entry points must not be confused.
- [`global_rf_would_over_ask_3_3`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___global_rf_would_over_ask_3_3): On `{3,3}` the global factor is six, so a `LOCAL_QUORUM` computed from it asks four acks where the local datacenter's three replicas need only two.
- [`quorum_2x3_matches_per_dc_sum`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_2x3_matches_per_dc_sum): `{3, 3}`: the global-RF threshold (4) coincides with `quorum_local_1 + quorum_local_2` (2 + 2).
- [`quorum_2x2_undershoots_per_dc_sum`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_2x2_undershoots_per_dc_sum): `{2, 2}`: the global-RF threshold (3) is *less* than `2 + 2`, so a transaction can be acknowledged without a quorum in the second DC.
- [`quorum_threshold_parity`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_threshold_parity): The general shape: for two uniform DCs of size `r`, the global threshold is `r + 1` while the per-DC sum is `2 * (r / 2 + 1)`, which agree iff `r` is odd.
- [`quorum_threshold_allows_uneven_split`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_threshold_allows_uneven_split): For `{3, 3}`, four acknowledgements satisfy QUORUM while dc2 has contributed only one — fewer than its own quorum of two.
- [`quorum_threshold_allows_2_1_split`](Ferrosa/Topology/ConsistencyLevels/#Ferrosa___Topology___ConsistencyLevels___quorum_threshold_allows_2_1_split): And on `{2, 2}`: three acks satisfy QUORUM, and the only way to reach three is both of dc1's replicas plus one of dc2's — a quorum in dc1 and a non-quorum in dc2.

## `Ferrosa.Accord.Protocol`

[The module page](Ferrosa/Accord/Protocol/)

- [`Replica.apply_store`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___Replica___apply_store): Applying a transaction folds its ops into the replica's keyspace, in order, so an earlier op in the same transaction is visible to a later one.
- [`Replica.apply_log`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___Replica___apply_log): Applying a transaction appends its id to the apply log and touches nothing that was already there, which is what later dep-waits read.
- [`foldl_apply_store`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___foldl_apply_store): Folding applies from any starting replica = folding the ops into its state.
- [`run_log_eq_fold`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___run_log_eq_fold): Running a log equals folding it, by induction.
- [`mem_iff_append`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___mem_iff_append): Any element of a list splits it as `a ++ x :: b`, the decomposition `Before` is defined over.
- [`before_append_singleton`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___before_append_singleton): Appending a transaction to a log leaves the relative order of the transactions already in it alone, so an order once established cannot be disturbed by a late arrival.
- [`LogOk.dep_applied_before`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___LogOk___dep_applied_before): Lemma 1 — a dep-respecting log never applies `τ` before one of its lower-timestamped dependencies.
- [`cfgScenarios_length`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___cfgScenarios_length): Three of the six permutations survive the guard; the three that would apply B before its dependency A are excluded.
- [`cfg_dep_ordered`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___cfg_dep_ordered): Lemma 2, exhaustively: in every dep-respecting log, the conflicting pair A (1) and B (2) appears in timestamp order.
- [`no_deps_all_orders_admissible`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___no_deps_all_orders_admissible): With the dependency edge removed, all six apply orders become admissible: dep-wait is the only thing pinning the order.
- [`all_scenarios_serializable`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___all_scenarios_serializable): Every permitted interleaving produces a serializable history.
- [`all_scenarios_strict`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___all_scenarios_strict): The same check with strictness: with no real-time information in the history (`invoke = complete = 0`) strictness is vacuous, so this only restates the serializability result — it is here to show the strict checker ac…
- [`zero_times_fail_strict`](Ferrosa/Accord/Protocol/#Ferrosa___Accord___zero_times_fail_strict): The strictness predicate is not free: with all timestamps zero the two transactions that were concurrent-by-the-clock are rejected, because the order that serializes them is not the order the clock asserts.

## `Ferrosa.Accord.Strict`

[The module page](Ferrosa/Accord/Strict/)

- [`deadline_ge_t0`](Ferrosa/Accord/Strict/#Ferrosa___Accord___deadline_ge_t0): The deadline never precedes the proposing timestamp itself, given a latency estimate no larger than the worst-case latency.
- [`deadline_monotone`](Ferrosa/Accord/Strict/#Ferrosa___Accord___deadline_monotone): A later timestamp gets a later deadline: the buffer preserves order.
- [`deadline_yields_dependency`](Ferrosa/Accord/Strict/#Ferrosa___Accord___deadline_yields_dependency): The point of the buffer.
- [`missing_dependency_means_premise_failed`](Ferrosa/Accord/Strict/#Ferrosa___Accord___missing_dependency_means_premise_failed): Contrapositive, in the form an operator cares about: a transaction that is *not* in the buffer despite a lower timestamp is evidence the deadline was set from stale skew or latency estimates — i.e.
- [`strict_iff`](Ferrosa/Accord/Strict/#Ferrosa___Accord___strict_iff): The checker-side counterpart, for the histories a run actually produces: `StrictlySerializable` is `Serializable` plus this.

## `Ferrosa.Accord.Serializable`

[The module page](Ferrosa/Accord/Serializable/)

- [`strict_implies_serializable`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___strict_implies_serializable): Strict implies serializable — the order that witnesses strictness also witnesses serializability.
- [`empty_strictly_serializable`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___empty_strictly_serializable): An empty history is trivially both.
- [`hFreshRead_strict`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hFreshRead_strict): A write followed by a read that observes it is strictly serializable: the order that runs the write first reproduces the read and respects the clock.
- [`hFreshRead_serial`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hFreshRead_serial): The same history in the `Prop` form, witnessed by the order that runs the write before the read.
- [`hStaleRead_serial_but_not_strict`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hStaleRead_serial_but_not_strict): Serializable but not strictly serializable — one theorem, two facts.
- [`hPhantom_not_serializable`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hPhantom_not_serializable): **Counterexample.** Counterexample: a read of a value nothing ever wrote is not serializable in any order.
- [`hReadYourWrites_ok`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hReadYourWrites_ok): A transaction whose read observes its own earlier append is strictly serializable: the checker applies the transaction's ops in order, so read-your-writes is part of atomicity rather than a separate guarantee.
- [`hReadYourWritesBroken_rejected`](Ferrosa/Accord/Serializable/#Ferrosa___Accord___hReadYourWritesBroken_rejected): **Counterexample.** Counterexample: and the same transaction observed not seeing its own append is rejected.

## `Ferrosa.Accord.History`

[The module page](Ferrosa/Accord/History/)

- [`bumpPast_gt_seq`](Ferrosa/Accord/History/#Ferrosa___Accord___Ts___bumpPast_gt_seq): A counter-proposal's `seq` field is strictly larger than the one it bumps past, so the replica cannot re-propose the number it has already rejected.
- [`bumpPast_gt`](Ferrosa/Accord/History/#Ferrosa___Accord___Ts___bumpPast_gt): The bumped timestamp is strictly later than the one it bumps past, which is what makes a counter-proposal supersede the proposal it saw rather than tie with it.
- [`distinct_nodes_ordered`](Ferrosa/Accord/History/#Ferrosa___Accord___Ts___distinct_nodes_ordered): Two transactions proposed by *different* nodes in the same nanosecond still differ, because `node` is the last field of the order (§6.2).
- [`State.runOps_append`](Ferrosa/Accord/History/#Ferrosa___Accord___State___runOps_append): Running op list `a` and then op list `b` leaves the same keyspace as running the concatenation `a ++ b` — the composition lemma the serializability argument rests on, since it is what lets a transaction's ops be repla…
- [`State.runOps_read_only`](Ferrosa/Accord/History/#Ferrosa___Accord___State___runOps_read_only): A read-only transaction cannot change the state.
- [`State.runOps_append_visible`](Ferrosa/Accord/History/#Ferrosa___Accord___State___runOps_append_visible): Appending `v` to `k` makes `v` the last element of that key's list.

## `Ferrosa.Accord.Recovery`

[The module page](Ferrosa/Accord/Recovery/)

- [`recovery_correct`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___recovery_correct): Correct recovery: ballot 3 with value 2 is the highest accepted, and 2 is the value the quorum actually chose.
- [`recovery_diverges`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___recovery_diverges): The divergence.
- [`recovery_wrong_value`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___recovery_wrong_value): **Counterexample.** Counterexample: the conflated acceptor proposes a value no quorum ever accepted.
- [`conflation_relabels_without_accept`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___conflation_relabels_without_accept): The precise defect: a Prepare moved the *accepted* ballot from 1 to 5 while the value stayed put.
- [`correct_ballot_invariant`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___correct_ballot_invariant): In the correct acceptor, an accepted value is always recorded at a ballot the acceptor had already promised — the relation §6.1 relies on.
- [`write_skew_not_serializable`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___write_skew_not_serializable): Not serializable: A must precede B (B did not see A's write to k0) and B must precede A (A did not see B's write to k1).
- [`dep_guard_restores_serializability`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___dep_guard_restores_serializability): With the dependency intact the write-skew pair is strictly serializable — B's read observes A's append — so it is the missing edge, not the checker, that made the unguarded history fail.
- [`write_skew_not_strict`](Ferrosa/Accord/Recovery/#Ferrosa___Accord___write_skew_not_strict): And the unguarded history is rejected by the strict checker too — dropping a dependency is not a licence, it is a defect.

## `Ferrosa.Accord.AccordMultiDcLean`

[The module page](Ferrosa/Accord/AccordMultiDcLean/)

- [`stale_commit_timestamp_reachable`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___stale_commit_timestamp_reachable): A transaction can be committed below the DC's watermark.
- [`apply_order_violation`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___apply_order_violation): I-27 fails in the module as written.
- [`swap_dc_antecedent_never_holds`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___swap_dc_antecedent_never_holds): I-30 is vacuous: no action of the module assigns `pendingConfig`.
- [`mem_toApply_committed`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___mem_toApply_committed): Anything the watermark lets through is committed.
- [`applied_implies_committed`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___applied_implies_committed): I-28 holds by construction: `appliedAccord` is a set that only ever gains committed transactions, so "applied an aborted transaction" is unreachable without any idempotence argument.
- [`per_dc_commit_index_never_advances`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___per_dc_commit_index_never_advances): The per-DC Raft layer is a stub: nothing in the module advances a commit index, so no Raft property of the multi-DC composition is checked.
- [`watermark_monotone_by_guard`](Ferrosa/Accord/AccordMultiDcLean/#Ferrosa___Accord___AccordMultiDcLean___watermark_monotone_by_guard): Watermark monotonicity is a guard, not a theorem: `AdvanceWatermark` requires `w ≥ hlcWatermark[d]`, so `[][watermark'[d] ≥ watermark[d]]` is true because the step is only enabled when it is.

## `Ferrosa.Raft.Obligations`

[The module page](Ferrosa/Raft/Obligations/)

- [`inv_initial`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_initial): The initial state satisfies the invariant.
- [`inv_pendingNext_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_pendingNext_step): The pending slot still names the next index after any step.
- [`inv_pendingAttempted_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_pendingAttempted_step): A returned ordered write is recorded against the pending index, whether it succeeded or failed: both stages set `attempted`, so the record never names an entry whose write did not return.
- [`inv_appliedLeAttempted_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_appliedLeAttempted_step): Publication never leads the write attempt.
- [`inv_attemptedBound_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_attemptedBound_step): The write attempt never runs more than one entry ahead of publication, because the pending slot holds the next index and nothing else can raise it.
- [`inv_latchClean_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_latchClean_step): A latched machine holds no job.
- [`inv_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_step): Every event preserves the invariant, including `cancel` (a no-op), `panic`, `crash` and `restart`.
- [`inv_run`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___inv_run): The invariant holds of every state a trace can reach from a state that satisfies it, by induction on the trace.
- [`applied_monotone_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___applied_monotone_step): One step never lowers the published index, provided the pending entry is not behind it — which the invariant gives, and which also holds for the pre-PR policy, where the pending index is the already-published one.
- [`pending_ge_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___pending_ge_step): The pending-entry condition survives a step, so monotonicity can be chained along a trace.
- [`applied_monotone`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___applied_monotone): MONOTONICITY (proved).
- [`applied_monotone_late`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___applied_monotone_late): Monotonicity from an empty machine, for the fenced publish policy: no trace of events can move the published index backwards.
- [`applied_advances_by_one`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___applied_advances_by_one): No event advances the published index by more than one, so a trace cannot skip an entry.
- [`late_publish_only_at_finish`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___late_publish_only_at_finish): GAPLESSNESS, part 1 (proved).
- [`publish_requires_completed_job`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___publish_requires_completed_job): GAPLESSNESS, part 2 (proved).
- [`publish_advances_to_pending_index`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___publish_advances_to_pending_index): GAPLESSNESS, part 3 (proved).
- [`latch_closed`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___latch_closed): LATCH-CLOSEDNESS (proved).
- [`latch_clears_pending`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___latch_clears_pending): The latch is never set while a job is retained, so a latched machine has no work left that could move state behind the error.
- [`publish_after_write_attempt`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___publish_after_write_attempt): Every published index has had its ordered system-table write return, so publication can never run ahead of the attempt.
- [`written_inv_initial`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___written_inv_initial): Nothing has been written and nothing attempted, so the durable prefix and the attempt agree on the empty machine.
- [`written_inv_step`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___written_inv_step): A successful ordered write raises the durable prefix and the attempt together.
- [`written_inv_run`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___written_inv_run): A trace in which no ordered write fails keeps the durable prefix level with the attempt, by induction on the trace.
- [`rows_present_when_writes_succeed`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___rows_present_when_writes_succeed): REPLAY SAFETY, durability half (proved, conditional).
- [`sysfail_publishes_without_row`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___sysfail_publishes_without_row): **Red.** REPLAY SAFETY, durability half — DISPROVED.
- [`cancel_is_noop`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___cancel_is_noop): Cancellation is not a state change.
- [`late_cancel_still_publishes`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___late_cancel_still_publishes): A cancelled apply is reaped and publishes with its ordered write.
- [`early_cancel_publishes_before_write`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___early_cancel_publishes_before_write): The pre-PR shape (DISPROVED).
- [`shared_exposes_effects_before_publication`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___shared_exposes_effects_before_publication): **Red.** ALIASING (disproved for the code as written).
- [`isolated_hides_effects_until_publication`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___isolated_hides_effects_until_publication): The same trace with owned handles touches nothing until publication, which is what sharing gives up.
- [`metadata_published_with_index`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___metadata_published_with_index): What sharing does not cost.
- [`panic_after_physical_latches`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___panic_after_physical_latches): ALIASING under a panicking job.
- [`raft_has_five_items_and_two_red`](Ferrosa/Raft/Obligations/#Ferrosa___Raft___raft_has_five_items_and_two_red): The area records five obligations of which two are unmet, and both numbers are pinned here: closing one, or adding a claim, forces this table to be updated in the same change.
-/
