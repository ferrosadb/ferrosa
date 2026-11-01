/-!
# Register VI: Evidence

A specification that only compiles has not been tested against a real history. This part is the machinery that closes that gap: Jepsen's checker models transcribed, the history type they read, the guarantee table, and two harnesses — one that runs every invariant over an enumerated case space, one that checks a recorded Elle history against the Accord model. A claim in this part names what would falsify it.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Testing.Jepsen`

[The module page](Ferrosa/Testing/Jepsen/)

- [`an_empty_run_establishes_nothing`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___an_empty_run_establishes_nothing): A run that observed nothing establishes nothing.
- [`a_local_read_run_cannot_establish_strict_serializability`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___a_local_read_run_cannot_establish_strict_serializability): A run whose reads are local across a failover cannot establish it, however many operations it observed.
- [`the_only_qualifying_run_shape`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___the_only_qualifying_run_shape): Only a run that witnessed reads and writes, kept the read-only transactions, and did not read locally across a failover licenses the claim.
- [`a_local_read_across_a_failover_produces_the_stale_read_shape`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___a_local_read_across_a_failover_produces_the_stale_read_shape): A multi-DC run with local reads and a failover produces exactly the history shape that is serializable but not strictly serializable: the read saw the pre-write value and real time says it came after the write.
- [`a_read_only_transaction_can_carry_the_only_cycle`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___a_read_only_transaction_can_carry_the_only_cycle): **Red.** The cycle is invisible if the read-only transaction is dropped.
- [`dropping_a_transaction_can_only_hide_violations`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___dropping_a_transaction_can_only_hide_violations): Removing edges never creates a cycle, so a checker that omits a transaction can only under-report violations, never over-report them.
- [`an_empty_graph_cannot_report_a_violation`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___an_empty_graph_cannot_report_a_violation): A graph with no edges has no cycle, so the vacuous case agrees with `an_empty_run_establishes_nothing`.
- [`an_absent_required_checker_must_fail_the_run`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___an_absent_required_checker_must_fail_the_run): **Red.** A required checker that did not run must fail the run.
- [`the_rules_agree_when_every_checker_ran`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___the_rules_agree_when_every_checker_ran): The two rules agree whenever every checker ran, so requiring the result costs nothing on a run that produced one.
- [`a_scraped_verdict_passes_an_invalid_result`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___a_scraped_verdict_passes_an_invalid_result): **Red.** A verdict searched for in the output passes an invalid result.
- [`a_lower_cased_anomaly_search_finds_nothing`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___a_lower_cased_anomaly_search_finds_nothing): And a case-folded search never finds an anomaly.
- [`jepsen_has_four_red_items`](Ferrosa/Testing/Jepsen/#Ferrosa___Testing___Jepsen___jepsen_has_four_red_items): The ledger carries four reds: the multi-key assertion and the read-only inclusion, because no transactional checker runs on a suite run; an absent checker counted as a pass; and a verdict read by searching text.

## `Ferrosa.Testing.JepsenModels`

[The module page](Ferrosa/Testing/JepsenModels/)

- [`counter_empty_is_valid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___counter_empty_is_valid): Empty history.
- [`counter_initial_read_is_zero`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___counter_initial_read_is_zero): `counter-test` "initial read": a fresh counter reads 0.
- [`counter_cannot_start_at_one`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___counter_cannot_start_at_one): `counter-test` "initial read" negative: the same history reading 1 is not linearizable.
- [`a_failed_add_need_not_have_taken_effect`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_failed_add_need_not_have_taken_effect): `counter-test` "ignore failed ops": a failed `add` need not have taken effect, so a later read of 0 is legal.
- [`an_incomplete_add_may_take_effect_between_reads`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___an_incomplete_add_may_take_effect_between_reads): `counter-test` "incomplete history": an unfinished `add 1` may take effect between the two reads, 0 then 1.
- [`an_incomplete_add_need_not_take_effect`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___an_incomplete_add_need_not_take_effect): And it need not: the same two reads are legal at 0 and 0.
- [`an_acknowledged_add_is_visible_to_a_later_read`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___an_acknowledged_add_is_visible_to_a_later_read): An acknowledged `add` must be visible to a read that starts after it completes.
- [`queue_empty_is_valid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___queue_empty_is_valid): `queue-test` "empty".
- [`a_possible_enqueue_alone_is_valid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_possible_enqueue_alone_is_valid): `queue-test` "Possible enqueue but no dequeue".
- [`a_definite_enqueue_alone_is_valid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_definite_enqueue_alone_is_valid): `queue-test` "Definite enqueue but no dequeue".
- [`a_dequeue_may_observe_a_concurrent_enqueue`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_dequeue_may_observe_a_concurrent_enqueue): `queue-test` "concurrent enqueue/dequeue": a dequeue can return a value enqueued concurrently.
- [`a_dequeue_without_an_enqueue_is_invalid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_dequeue_without_an_enqueue_is_invalid): `queue-test` "dequeue but no enqueue": a dequeue of a value never enqueued is invalid.
- [`a_queue_cannot_hand_out_more_than_it_was_given`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___a_queue_cannot_hand_out_more_than_it_was_given): A value can be dequeued at most as many times as it was enqueued.
- [`total_queue_empty_is_valid`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___total_queue_empty_is_valid): `total-queue-test` "empty".
- [`total_queue_dequeues_in_fifo_order`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___total_queue_dequeues_in_fifo_order): `total-queue-test` "sane": enqueue 1 and 2, dequeue 1 then 2.
- [`total_queue_rejects_out_of_order_dequeues`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___total_queue_rejects_out_of_order_dequeues): The same history with the dequeue order reversed is not FIFO.
- [`overlapping_enqueues_leave_the_fifo_order_free`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___overlapping_enqueues_leave_the_fifo_order_free): The same history is legal when the enqueues overlap, because then either enqueue order is possible and the FIFO model can fit the observations either way.
- [`the_queue_models_differ_on_the_same_history`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___the_queue_models_differ_on_the_same_history): **Red.** The two queue models disagree, which is why the choice of model is part of the test: the same history is legal for an unordered queue and illegal for a total one.
- [`models_has_three_red_items`](Ferrosa/Testing/JepsenModels/#Ferrosa___Testing___Models___models_has_three_red_items): Three reds, one per unported family: the set model with its multi-value reads, set-full with unique-ids, and the generators and nemeses.

## `Ferrosa.Testing.Histories`

[The module page](Ferrosa/Testing/Histories/)

- [`cnt_append`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___cnt_append): A value's multiplicity in a concatenation is the sum of its multiplicities in the parts, which is what lets the bookkeeping treat the order of a multiset as irrelevant.
- [`multiset_union_is_commutative`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___multiset_union_is_commutative): Union is commutative as a multiset, even though concatenation is not commutative as a list.
- [`an_acknowledged_write_that_is_read_back_is_not_lost`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___an_acknowledged_write_that_is_read_back_is_not_lost): A write that was acknowledged and then read back is not lost.
- [`an_acknowledged_write_that_is_never_read_back_is_lost`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___an_acknowledged_write_that_is_never_read_back_is_lost): A write acknowledged and never read back is lost.
- [`a_value_that_no_write_acknowledged_is_unexpected`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_value_that_no_write_acknowledged_is_unexpected): A value the read returns without any acknowledgement is unexpected — nothing in the run explains where it came from.
- [`a_loss_and_a_fabrication_do_not_cancel`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_loss_and_a_fabrication_do_not_cancel): **Red.** `lost` and `fabricated` are independent, so they cannot cancel: a run can lose one value and fabricate another, and the two multisets stay separate.
- [`a_timed_out_write_that_landed_is_not_fabricated`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_timed_out_write_that_landed_is_not_fabricated): **Red.** A timed-out write that landed is not a fabrication.
- [`a_definite_failure_that_landed_is_fabricated`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_definite_failure_that_landed_is_fabricated): A write that failed definitively and was read back is a fabrication.
- [`an_indeterminate_write_never_hides_a_loss`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___an_indeterminate_write_never_hides_a_loss): An indeterminate write never hides a loss.
- [`a_missing_write_with_a_clean_read_is_a_silent_loss`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_missing_write_with_a_clean_read_is_a_silent_loss): A clean read plus a missing write is the silent-loss shape.
- [`a_fabrication_alone_is_not_silent_loss`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_fabrication_alone_is_not_silent_loss): A run that lost nothing is not a silent loss, however many values it fabricated.
- [`a_run_without_acknowledged_writes_cannot_lose_anything`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_run_without_acknowledged_writes_cannot_lose_anything): A run that acknowledged nothing cannot lose anything.
- [`a_run_without_a_final_read_loses_every_acknowledged_write`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_run_without_a_final_read_loses_every_acknowledged_write): **Red.** And the converse: a run that read nothing cannot have recovered anything, so every acknowledged write counts as lost.
- [`nothing_is_confirmed_that_the_profile_cannot_support`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___nothing_is_confirmed_that_the_profile_cannot_support): No over-claim: a run is only ever `confirmed` when its profile could see every acknowledged write.
- [`nothing_is_excused_that_the_profile_can_convict`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___nothing_is_excused_that_the_profile_can_convict): No under-claim: when the profile can see every acknowledged write, an anomaly is `refuted` rather than excused.
- [`a_clean_book_under_one_one_is_not_confirmed`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_clean_book_under_one_one_is_not_confirmed): A run under CL=ONE/CL=ONE on three nodes is not confirmed, however clean the book looks.
- [`the_same_clean_book_under_quorum_is_confirmed`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___the_same_clean_book_under_quorum_is_confirmed): The same book under CL=QUORUM/CL=QUORUM is confirmed.
- [`a_landed_timeout_under_quorum_is_confirmed`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_landed_timeout_under_quorum_is_confirmed): Under CL=QUORUM a timed-out write that landed does not refute the run; it is reported, and the run is confirmed on the strength of everything that was acknowledged.
- [`a_lost_write_under_all_all_is_refuted`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_lost_write_under_all_all_is_refuted): And under CL=ALL/CL=ALL a lost write is refuted, not excused.
- [`a_cross_datacenter_read_is_inconclusive_at_every_level`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_cross_datacenter_read_is_inconclusive_at_every_level): A cross-datacenter read is inconclusive even at CL=ALL, and the reason is recorded rather than collapsed into "not tested".
- [`a_sequential_generator_cannot_expose_an_ordering_anomaly`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_sequential_generator_cannot_expose_an_ordering_anomaly): A generator that never overlaps two operations cannot convict an ordering model.
- [`a_write_free_generator_cannot_test_durability`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_write_free_generator_cannot_test_durability): A generator that never writes cannot lose anything, so a green durability result from it is vacuous.
- [`a_generator_without_a_final_read_cannot_measure_loss`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_generator_without_a_final_read_cannot_measure_loss): A generator that omits the final read cannot detect a loss at all: every acknowledged write counts as lost, so the run is either red for the wrong reason or — if it never wrote — green for no reason.
- [`an_ack_before_the_crash_is_a_durability_promise`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___an_ack_before_the_crash_is_a_durability_promise): A crash after the acknowledgement excuses nothing, because the acknowledgement was the promise to survive it; a crash before the acknowledgement is the only case that does.
- [`a_missing_write_after_a_crash_is_only_excusable_if_the_crash_was_logged`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___a_missing_write_after_a_crash_is_only_excusable_if_the_crash_was_logged): An attribution needs both a fault log and a crash that preceded the acknowledgement: an unlogged fault, or one that arrived after the promise, leaves the missing value unexplained.
- [`histories_has_three_red_items`](Ferrosa/Testing/Histories/#Ferrosa___Testing___Histories___histories_has_three_red_items): Three reds in this ledger, all for one reason: no Rust workload keeps the book, records which writes were indeterminate, or issues a final read, so the multisets this file reasons about are never computed for a real run.

## `Ferrosa.Testing.Guarantees`

[The module page](Ferrosa/Testing/Guarantees/)

- [`one_and_all_do_not_share_a_durability_guarantee`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___one_and_all_do_not_share_a_durability_guarantee): On a three-node cluster, CL=ONE and CL=ALL do not share a durability guarantee, by design: one acknowledged replica is not two, and a single-node loss is exactly what CL=ONE does not survive.
- [`a_one_and_one_configuration_cannot_report_loss`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_one_and_one_configuration_cannot_report_loss): CL=ONE plus CL=ONE does not overlap on three nodes: a read can miss an acknowledged write, so a missing value is not evidence of anything.
- [`a_quorum_pair_overlaps_on_three_nodes`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_quorum_pair_overlaps_on_three_nodes): Quorum plus quorum overlaps on three nodes.
- [`an_all_write_with_a_one_read_still_overlaps`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___an_all_write_with_a_one_read_still_overlaps): It is the pair that matters, not either level.
- [`a_one_write_with_a_quorum_read_does_not_overlap`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_one_write_with_a_quorum_read_does_not_overlap): And CL=ONE writes with CL=QUORUM reads do not, on three nodes: 1 + 2 is not greater than 3.
- [`raising_the_read_level_cannot_remove_a_guarantee`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___raising_the_read_level_cannot_remove_a_guarantee): Raising either level can only add guarantees.
- [`raising_the_write_level_cannot_remove_a_guarantee`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___raising_the_write_level_cannot_remove_a_guarantee): Raising the write level by one cannot break the overlap, so a configuration that could see every acknowledged write still can after the change.
- [`all_implies_one_for_durability`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___all_implies_one_for_durability): A stronger level implies every weaker guarantee, so the guarantee set is a chain and the suite can assert the strongest property and know it covers the weaker configurations.
- [`a_cross_datacenter_read_defeats_the_overlap`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_cross_datacenter_read_defeats_the_overlap): DC_QUORUM's local read cannot see a write acknowledged elsewhere, even with an overlapping replica count, because the overlap is an argument about one replica set.
- [`a_datacenter_quorum_read_restores_the_overlap`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_datacenter_quorum_read_restores_the_overlap): A read that consults a quorum of datacenters restores it: the guarantee is about the read's *routing*, and no level name on its own expresses that.
- [`a_missing_value_under_one_one_is_not_evidence`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_missing_value_under_one_one_is_not_evidence): Under CL=ONE/CL=ONE a missing value is not evidence.
- [`the_same_missing_value_under_quorum_quorum_is_evidence`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___the_same_missing_value_under_quorum_quorum_is_evidence): Under CL=QUORUM/CL=QUORUM the same missing value is evidence.
- [`a_verdict_without_the_profile_is_not_a_verdict`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___a_verdict_without_the_profile_is_not_a_verdict): The same history has different verdicts under different configurations, so a verdict is a function of `(history, profile)` and a pass/fail without the profile is not a result.
- [`guarantees_has_no_red_items`](Ferrosa/Testing/Guarantees/#Ferrosa___Testing___Guarantees___guarantees_has_no_red_items): No red rows: every claim this ledger makes about the consistency levels is proved, and this pins that none has been added since without a proof.

## `Ferrosa.TlaCheck`

[The module page](Ferrosa/TlaCheck/)

- [`prevote_guard_is_vacuous`](Ferrosa/TlaCheck/#Ferrosa___TlaCheck___prevote_guard_is_vacuous): The invariant as written in `raft.tla:283-286`: ``` NoTermAdvanceWithoutPreVoteMajority == \A i \in Server : state[i] = Candidate => preVotesGranted[i] \in Quorum(i) \/ TRUE ``` `P ∨ TRUE` is `TRUE`.
- [`candidate_without_prevote_reachable`](Ferrosa/TlaCheck/#Ferrosa___TlaCheck___candidate_without_prevote_reachable): A Candidate with zero PreVotes is reachable.
- [`config_never_propagates`](Ferrosa/TlaCheck/#Ferrosa___TlaCheck___config_never_propagates): The joint configuration never reaches the followers.
- [`leader_can_vote_without_stepping_down`](Ferrosa/TlaCheck/#Ferrosa___TlaCheck___leader_can_vote_without_stepping_down): The model lets a Leader vote for someone else and keep leading.
-/
