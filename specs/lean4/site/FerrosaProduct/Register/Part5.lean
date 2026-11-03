/-!
# Register V: Foundations

Reference, not an introduction, kept near the back deliberately: the counting lemma every quorum proof rests on, the vocabulary the topology rungs are stated in, and the ledger that records what is proved and what is not. Nothing in this part describes anything a user can observe — it is the apparatus the four parts above are written with, and the place to look when a term in an earlier chapter needs defining.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Core`

[The module page](Ferrosa/Core/)

- [`card_univ`](Ferrosa/Core/#Ferrosa___SetU___card_univ): Every node counts: the whole cluster, enumerated, is `n` nodes.
- [`card_empty`](Ferrosa/Core/#Ferrosa___SetU___card_empty): No node is in the empty set, so a set that admits nobody admits zero nodes.
- [`filter_and_length_le`](Ferrosa/Core/#Ferrosa___filter_and_length_le): Two predicates over one finite list cannot tally more than the list plus their overlap — the step that forces two strict majorities of the same universe to share a node.
- [`quorum_intersects`](Ferrosa/Core/#Ferrosa___quorum_intersects): Two strict majorities of the same universe always share a node.
- [`no_two_disjoint_quorums`](Ferrosa/Core/#Ferrosa___no_two_disjoint_quorums): Contrapositive form, used when reasoning about *unavailability* or divergence: two disjoint ack sets cannot both be quorums.
- [`quorum_wr_intersects`](Ferrosa/Core/#Ferrosa___quorum_wr_intersects): The `W + R > RF` theorem: any write-ack set of size `w` and any read-ack set of size `r` share a node, hence a quorum read observes the write.
- [`blockFor_at_least_one`](Ferrosa/Core/#Ferrosa___blockFor_at_least_one): Every threshold but EACH_QUORUM's placeholder names at least one ack.
- [`blockFor_le_rf`](Ferrosa/Core/#Ferrosa___blockFor_le_rf): Thresholds never exceed `rf` — the `block_for_never_exceeds_rf` proptest in `consistency.rs`, restated over all `rf`.
- [`localQuorum_global_rf_infeasible`](Ferrosa/Core/#Ferrosa___localQuorum_global_rf_infeasible): LOCAL_QUORUM computed against the global RF needs more acks than a single DC can ever supply.
- [`localQuorum_threshold_path_mismatch`](Ferrosa/Core/#Ferrosa___localQuorum_threshold_path_mismatch): The same consistency level yields two different thresholds depending on which coordinator entry point serves the request.
- [`rf2_quorum_requires_all`](Ferrosa/Core/#Ferrosa___rf2_quorum_requires_all): `RF = 2`: QUORUM needs every replica, so there is no fault tolerance.
- [`rf3_quorum_is_two`](Ferrosa/Core/#Ferrosa___rf3_quorum_is_two): `RF = 3`: QUORUM needs two of three, tolerating exactly one replica loss.
- [`rf5_quorum_is_three`](Ferrosa/Core/#Ferrosa___rf5_quorum_is_three): `RF = 5`: QUORUM needs three, tolerating two replica losses.
- [`rf3_quorum_quorum_intersects`](Ferrosa/Core/#Ferrosa___rf3_quorum_quorum_intersects): At `RF = 3`, `QUORUM/QUORUM` satisfies `W + R > RF` (2 + 2 > 3).
- [`rf2_one_one_no_intersection`](Ferrosa/Core/#Ferrosa___rf2_one_one_no_intersection): At `RF = 2`, `ONE/ONE` does not satisfy `W + R > RF` (1 + 1 ≤ 2): a `ONE` read may miss a `ONE` write that acked on the other replica.

## `Ferrosa.Ledger`

[The module page](Ferrosa/Ledger/)

- [`isRed_eq_status_test`](Ferrosa/Ledger/#Ferrosa___Ledger___isRed_eq_status_test): `isRed` is the predicate `redCount` counts, spelled as a function.
- [`redCount_eq_filter_length`](Ferrosa/Ledger/#Ferrosa___Ledger___redCount_eq_filter_length): The red count is the length of the ledger filtered to its red items, so counting and filtering cannot disagree about how many obligations an area has left open.
- [`redNames_length`](Ferrosa/Ledger/#Ferrosa___Ledger___redNames_length): The names of the red items are as many as the red count, so the list a classification is keyed on covers every unmet obligation and no discharged one.

## `Ferrosa.Commons.Tactics`

[The module page](Ferrosa/Commons/Tactics/)

- [`merge_comm`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___merge_comm): Pointwise maximum does not care which argument comes first — the law `Storage.Tombstone` uses to converge two replicas whatever order they merge in, and `Commons.Hll` to fold partial sketches.
- [`merge_assoc`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___merge_assoc): Grouping is irrelevant to pointwise maximum, so a fan-in of merges needs no canonical tree shape.
- [`merge_idem`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___merge_idem): Merging a value with itself returns it, which is what makes a retried merge harmless: it cannot change a tombstone's choice or inflate a sketch register.
- [`merge_ext`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___merge_ext): Two pointwise-max merges agree if they agree register by register.
- [`min_add_sub`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___min_add_sub): Resident plus spilled equals the backlog: what a buffer still holds and what it moved to disk together account for everything pushed, which is the identity the spill bounds in `Cdc.Subscribe` and `Storage.Streaming` a…
- [`min_le_left`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___min_le_left): A minimum is no greater than its first argument: a buffer never holds more than either the backlog or its share.
- [`min_le_right`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___min_le_right): A minimum is no greater than its second argument, so resident bytes stay inside the allocation as well as inside the backlog.
- [`a_bounded_share_stays_inside_its_bound`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___a_bounded_share_stays_inside_its_bound): Once a backlog is inside its share, it stays inside.
- [`resident_does_not_rise_when_the_backlog_does_not`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___resident_does_not_rise_when_the_backlog_does_not): Resident bytes do not rise while the backlog does not.
- [`a_share_inside_the_pool_stays_inside`](Ferrosa/Commons/Tactics/#Ferrosa___Commons___Tactics___a_share_inside_the_pool_stays_inside): A consumer holding at most its share cannot exceed the pool if the share cannot.

## `Ferrosa.Topology.Basic`

[The module page](Ferrosa/Topology/Basic/)

- [`Topo.okAtB_iff`](Ferrosa/Topology/Basic/#Ferrosa___Topology___Topo___okAtB_iff): The Boolean admission test agrees with the propositional one for every placement, failure pattern and consistency level, so an answer obtained by enumerating failures is an answer about the predicate and not a separat…
- [`Topo.localOkAtB_iff`](Ferrosa/Topology/Basic/#Ferrosa___Topology___Topo___localOkAtB_iff): The same agreement for the per-datacenter gate, so a failure pattern enumerated for a `LOCAL_*` level answers the question `localOkAt` asks.
-/
