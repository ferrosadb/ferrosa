/-!
# Register I: The query surface

Start here if you are using Ferrosa rather than building it. Four front ends — CQL, SQL, SPARQL and the Bolt session — over the same storage and the same consistency rules. The claims are about what a query means: which statements are refused at parse time, what an aggregate returns when a shard is unreachable, and how a subscription's order relates to the write path. Nothing here asks you to know how a write is stored.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Cql.Classify`

[The module page](Ferrosa/Cql/Classify/)

- [`blockFor_one_at_one`](Ferrosa/Cql/Classify/#Ferrosa___Cql___blockFor_one_at_one): A keyspace holding one replica needs one acknowledgement at `ONE`, so the level adds nothing to what the topology already requires.
- [`blockFor_all_at_one`](Ferrosa/Cql/Classify/#Ferrosa___Cql___blockFor_all_at_one): With one replica, `ALL` asks for that replica and so for a single acknowledgement — the same number as `ONE`.
- [`plan_rf_one_single_ack`](Ferrosa/Cql/Classify/#Ferrosa___Cql___plan_rf_one_single_ack): `RF = 1` needs one ack at every level that applies to a single-DC keyspace — the front end adds nothing to the topology rule.
- [`plan_local_quorum_is_local`](Ferrosa/Cql/Classify/#Ferrosa___Cql___plan_local_quorum_is_local): `LOCAL_QUORUM` names exactly one DC and requires that DC's quorum.
- [`plan_quorum_nts_is_per_dc`](Ferrosa/Cql/Classify/#Ferrosa___Cql___plan_quorum_nts_is_per_dc): `QUORUM` on NTS requires a quorum in each DC, and the total is the sum of those — not a global majority.
- [`code_plan_agrees_only_for_odd_dc_rf`](Ferrosa/Cql/Classify/#Ferrosa___Cql___code_plan_agrees_only_for_odd_dc_rf): Where the implementation's plan differs from the specified one.
- [`local_levels_have_a_dc_requirement`](Ferrosa/Cql/Classify/#Ferrosa___Cql___local_levels_have_a_dc_requirement): A level that names a DC always produces a per-DC requirement.
- [`plan_all_is_total`](Ferrosa/Cql/Classify/#Ferrosa___Cql___plan_all_is_total): `ALL` on NTS requires every replica, whatever the level's "quorum" arithmetic would have said.
- [`resolveCl_of_some`](Ferrosa/Cql/Classify/#Ferrosa___Cql___resolveCl_of_some): A level carried in the frame is used as sent: the session default never overrides a client's explicit choice.
- [`resolveCl_of_none`](Ferrosa/Cql/Classify/#Ferrosa___Cql___resolveCl_of_none): A query that names no level runs at the session default, so a client that says nothing gets the configured level rather than an error.
- [`conditional_reads_its_keys`](Ferrosa/Cql/Classify/#Ferrosa___Cql___conditional_reads_its_keys): A conditional statement reads the keys it compares against.
- [`conditional_contributes_condition`](Ferrosa/Cql/Classify/#Ferrosa___Cql___conditional_contributes_condition): And it contributes exactly one condition.
- [`unconditional_has_no_condition`](Ferrosa/Cql/Classify/#Ferrosa___Cql___unconditional_has_no_condition): An unconditional write contributes no condition.
- [`any_conditional_iff`](Ferrosa/Cql/Classify/#Ferrosa___Cql___any_conditional_iff): A group of statements tests a condition exactly when one of its members does, which is what lets a batch's shape be read off its statements.
- [`conditional_txn_not_blind`](Ferrosa/Cql/Classify/#Ferrosa___Cql___conditional_txn_not_blind): LWT needs a read phase.

## `Ferrosa.Cql.Collections`

[The module page](Ferrosa/Cql/Collections/)

- [`setAdd_comm`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___setAdd_comm): Set adds commute — the whole reason a set survives last-write-wins.
- [`setAdd_idem`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___setAdd_idem): Set adds are idempotent, so a replayed add is harmless.
- [`set_lww_is_order_free`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___set_lww_is_order_free): A set's merge is a union: order-free, and equal to the serial result.
- [`listSnoc_not_comm`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___listSnoc_not_comm): List appends do not commute.
- [`list_lww_loses_an_element`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___list_lww_loses_an_element): The lost update.
- [`set_lww_loses_nothing`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___set_lww_loses_nothing): The same two appends as a set: nothing is lost, in either merge order.
- [`collection_append_classified_blind`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___collection_append_classified_blind): Classified blind.
- [`collection_append_must_read`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___collection_append_must_read): But it must read.
- [`blind_classification_is_term_blind`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___blind_classification_is_term_blind): The inconsistency, as one statement.
- [`scalar_assignment_is_genuinely_blind`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___scalar_assignment_is_genuinely_blind): A plain scalar assignment (`SET name = 'b'`) reads nothing of its own, so the blind classification is right for it — the classifier is not uniformly wrong, which is why the fix belongs at the term level rather than by…
- [`blindSem_rejects_collection_append`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___blindSem_rejects_collection_append): The tightened classifier refuses a collection append, so `tags = tags + {'x'}` takes the read phase and cannot be acknowledged on the write path alone.
- [`blindSem_accepts_scalar`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___blindSem_accepts_scalar): A plain scalar assignment still qualifies as blind under the tightened classifier, so ordinary writes keep the fast path.
- [`blindSem_is_stricter`](Ferrosa/Cql/Collections/#Ferrosa___Cql___Collections___blindSem_is_stricter): `blindSem` never admits something `blind` rejects: it is a restriction of the implemented classifier, so tightening it cannot open a fast path that was closed.

## `Ferrosa.Cql.Conformance`

[The module page](Ferrosa/Cql/Conformance/)

- [`qSelect_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qSelect_classified): `SELECT * FROM users WHERE id = 1` reads without writing, tests no condition, and therefore takes no blind path.
- [`qInsert_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qInsert_classified): A plain `INSERT` writes without reading anything first, so it qualifies as a blind write.
- [`qInsertIf_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qInsertIf_classified): `INSERT … IF NOT EXISTS` reads the partition to test its condition, so it is not a blind write.
- [`qUpdateIf_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qUpdateIf_classified): `UPDATE … IF name = 'a'` reads, writes and tests a condition, so it needs the read phase.
- [`qDeleteIf_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qDeleteIf_classified): `DELETE … IF EXISTS` reads, writes and tests a condition, so it needs the read phase.
- [`qBatchBlind_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qBatchBlind_classified): A batch of two blind inserts is itself blind: no member reads and none tests a condition, so the whole batch can be acknowledged on the write path.
- [`qBatchMixed_classified`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___qBatchMixed_classified): One conditional statement is enough to make the whole batch non-blind.
- [`plan_quorum_3_3`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_quorum_3_3): `QUORUM` on `{3,3}`: 2 + 2, and the plan says so per DC.
- [`plan_local_quorum_3_3`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_local_quorum_3_3): `LOCAL_QUORUM` in dc1: 2 acks, all from dc1.
- [`plan_all_3_3`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_all_3_3): `ALL` on `{3,3}`: all six.
- [`plan_each_quorum_3_3`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_each_quorum_3_3): `EACH_QUORUM` on `{3,3}`: a quorum in each DC, 4 in total — the same shape as `QUORUM`'s per-DC plan, which is why the two are easy to conflate.
- [`plan_one_rf1`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_one_rf1): `RF = 1`, `ONE`: one ack.
- [`plan_local_quorum_rf1`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_local_quorum_rf1): `RF = 1`, `LOCAL_QUORUM`: still one ack, still local — the front end adds nothing to the topology rule.
- [`plan_quorum_2_2_differs`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___plan_quorum_2_2_differs): The `{2,2}` discrepancy, in plan form.
- [`txn_shape_of_blind_pair`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___txn_shape_of_blind_pair): Two blind inserts between `BEGIN` and `COMMIT` give a transaction with nothing in its read set and no condition, which is the shape Accord can commit on the write path.
- [`txn_shape_with_condition`](Ferrosa/Cql/Conformance/#Ferrosa___Cql___txn_shape_with_condition): Adding one conditional statement puts a key in the read set and a condition in the shape — the front-end half of "LWT needs a transaction".

## `Ferrosa.Cql.Aggregate`

[The module page](Ferrosa/Cql/Aggregate/)

- [`estimate_is_the_only_inexact`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___estimate_is_the_only_inexact): The estimate is the only one whose answer may be short.
- [`foldCount_eq`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___foldCount_eq): The accumulator equals the row count.
- [`count_forgets_which_rows`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___count_forgets_which_rows): It forgets which rows.
- [`walkExactPair_fst`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___walkExactPair_fst): The counter and the visited set stay in step: whenever they start equal, the running count after a pass over the rows is the size of the set it ends with.
- [`exact_distinct_is_cardinality`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___exact_distinct_is_cardinality): `COUNT(DISTINCT)` from the empty map is the cardinality of the values seen.
- [`Bloom.insert_mem`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___Bloom___insert_mem): Inserting sets the probed bits.
- [`Bloom.no_false_negatives`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___Bloom___no_false_negatives): A Bloom filter has no false negatives.
- [`Bloom.member_mono`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___Bloom___member_mono): Setting bits cannot unset them: membership is monotone in the state.
- [`walkFrom_estimate_le`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___walkFrom_estimate_le): The invariant that makes the estimate a lower bound.
- [`estimate_le_exact`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___estimate_le_exact): The estimate is a lower bound on the exact count.
- [`estimate_can_be_strict`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___estimate_can_be_strict): Two distinct values colliding on the same single probe: `walkFrom` counts one, the exact walk counts two.
- [`exact_and_estimate_not_interchangeable`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___exact_and_estimate_not_interchangeable): The separation, as a theorem.
- [`estimate_exact_without_collision`](Ferrosa/Cql/Aggregate/#Ferrosa___Cql___Aggregate___estimate_exact_without_collision): And on an input with no collision the two agree, so the gap is the false-positive rate rather than a defect: `estimate = exact` when the filter is honest.

## `Ferrosa.Cql.AggregateObligations`

[The module page](Ferrosa/Cql/AggregateObligations/)

- [`count_star_is_exact`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___count_star_is_exact): `COUNT(*)` over the rows is their number, so a row count never has to be estimated or approximated.
- [`count_star_retains_no_row`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___count_star_retains_no_row): Two row sets of the same length produce the same `COUNT(*)`, so the answer carries nothing about any individual row.
- [`model_visited_set_length_is_the_count`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___model_visited_set_length_is_the_count): **Red.** The model holds one entry per distinct value, so the count and the memory the visited set occupies are the same number.
- [`sum_of_shard_counts_overcounts`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___sum_of_shard_counts_overcounts): **Red.** Summing per-shard counts over-counts.
- [`shard_sum_overcounts_by_the_overlap`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___shard_sum_overcounts_by_the_overlap): The same with several values, so the gap is not an artefact of a single row.
- [`bloom_bits_do_not_determine_the_count`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___bloom_bits_do_not_determine_the_count): **Red.** The bits do not determine the count.
- [`sum_of_estimates_overcounts`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___sum_of_estimates_overcounts): And summing per-shard estimates over-counts on the overlap, exactly as the exact counts do.
- [`exactness_does_not_hold`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___exactness_does_not_hold): **Red.** The obligation is not satisfied for `ε = 0`: a colliding filter under-counts.
- [`ledger_has_six_red_items`](Ferrosa/Cql/AggregateObligations/#Ferrosa___Cql___AggregateObligations___ledger_has_six_red_items): Six of the ledger's ten items are red, and the count is pinned here so a CI check can require it to fall and never to rise.

## `Ferrosa.Cql.Lwt`

[The module page](Ferrosa/Cql/Lwt/)

- [`applied_writes`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___applied_writes): An applied CAS wrote its value.
- [`not_applied_leaves_state`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___not_applied_leaves_state): A failed CAS changed nothing — the aborted-transaction rule.
- [`applied_iff_cond`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___applied_iff_cond): The applied flag is exactly the condition.
- [`insert_if_not_exists_on_empty`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___insert_if_not_exists_on_empty): `IF NOT EXISTS` on a fresh key applies; `IF v = x` on a fresh key does not.
- [`update_if_on_empty_fails`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___update_if_on_empty_fails): `UPDATE … IF v = v0` on a key that holds nothing is rejected, so a conditional update never creates the row it conditions on.
- [`at_most_one_applies`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___at_most_one_applies): The CAS guarantee.
- [`at_most_one_applies_either_order`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___at_most_one_applies_either_order): The same statement the other way round: whichever runs first wins, and at most one of the two ever applies, in either order.
- [`failed_cas_reports_the_state_it_tested`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___failed_cas_reports_the_state_it_tested): A failed CAS reports the state it tested.
- [`unprotected_means_keys_differ`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___unprotected_means_keys_differ): The `UnprotectedLwt` shape can only be built when the two keys differ; if they coincide it *is* a protected CAS.
- [`unprotected_cas_is_not_serializable`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___unprotected_cas_is_not_serializable): Not serializable.
- [`protected_cas_is_serializable`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___protected_cas_is_serializable): A run in which the second conditional write observes the first one's value serializes in timestamp order, so the checker accepts it.
- [`cas_with_failure_is_serializable`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___cas_with_failure_is_serializable): A run of two conditional writes on one key, where the second is rejected, is serializable: only the first is in the serial order, and the rejected one wrote nothing.
- [`failed_cas_observes_the_committed_value`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___failed_cas_observes_the_committed_value): The failed CAS observed exactly what the committed CAS wrote — the `CurrentValues` contract, checked on the history rather than asserted.
- [`protected_cas_reads_its_write_key`](Ferrosa/Cql/Lwt/#Ferrosa___Cql___Lwt___protected_cas_reads_its_write_key): A conditional statement's operations include a read of the key it conditions on, which is what makes the condition protected rather than a stale read followed by a blind write.

## `Ferrosa.Cql.Refine`

[The module page](Ferrosa/Cql/Refine/)

- [`cellSet_eq`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___cellSet_eq): The local `SET` and the specification's are the same operation, by definition: the refinement wrapper adds no behaviour of its own, only a name in this module's vocabulary.
- [`lwtCond_eq`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___lwtCond_eq): The local condition test is the specification's, by definition — so a conditional statement's decision here is the same decision the transaction spec makes there.
- [`lwtExec_eq`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___lwtExec_eq): Executing a conditional here is executing it there, by definition; the pair returned is the cell and whether the condition held.
- [`last_encode_none`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___last_encode_none): Encoding a cell that holds nothing at a key gives a keyspace whose list for that key is empty, so its last value is nothing.
- [`last_encode_some`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___last_encode_some): Encoding a cell that holds `v` at a key gives a keyspace whose last value at that key is `v`.
- [`encode_simulates`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___encode_simulates): `encode` simulates the cell it came from.
- [`decode_encode`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___decode_encode): The abstraction is a retraction: decoding an encoded cell returns the cell exactly — the direction that says the encoding loses nothing the scalar model can express.
- [`decode_unique`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___decode_unique): The abstraction is well defined: any two states that simulate a cell agree on it, so `decode` is *the* abstraction, not one choice among several.
- [`simulates_decode`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___simulates_decode): Every state simulates its own decoding — the relation is total on the right.
- [`State.runOps_singleton`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___State___runOps_singleton): Running a single op is stepping once.
- [`getLast?_append_singleton`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___getLast____append_singleton): The append list grows by exactly one element at the end.
- [`step_append_other`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___step_append_other): An append to `k` leaves every other key alone.
- [`step_append_self`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___step_append_self): An append to `k` makes `v` the last element of its list.
- [`simulates_append`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___simulates_append): The transition refines.
- [`simulates_append_far`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___simulates_append_far): Frames survive: a state still simulates a cell after an append somewhere the cell does not look.
- [`condOn_eq_cond`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___condOn_eq_cond): The condition agrees across the two models whenever they simulate each other: the check the CAS performs on the cell is the check it performs on the keyspace.
- [`lwt_refines`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___lwt_refines): An LWT refines its cell-level execution.
- [`failed_lwt_leaves_keyspace`](Ferrosa/Cql/Refine/#Ferrosa___Cql___Refine___failed_lwt_leaves_keyspace): A failed LWT leaves the keyspace unchanged — the abort rule at the keyspace level, so a rejected CAS cannot make a replica diverge.

## `Ferrosa.Cql.Index`

[The module page](Ferrosa/Cql/Index/)

- [`reorder_never_filters`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___reorder_never_filters): The selection never filters.
- [`first_segment_is_ready`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___first_segment_is_ready): The first segment really is the ready set — the preference is not a fiction.
- [`all_building_returns_all`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___all_building_returns_all): The all-building case, mirroring the Rust test of the same name: when nobody has finished the build, the selection returns the input unchanged.
- [`preference_is_not_completeness`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___preference_is_not_completeness): Preference is not completeness.
- [`all_ready_is_complete`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___all_ready_is_complete): The positive direction: if every replica is ready, the selection is complete — so the `Ready` fast path is sound, and the gap is confined to the degraded case.
- [`indexed_read_can_be_stale`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___indexed_read_can_be_stale): Not serializable.
- [`indexed_read_when_fresh_is_serializable`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___indexed_read_when_fresh_is_serializable): The same history with the row visible to the indexed read serializes, so it is the staleness and not the index that the checker rejects.
- [`index_entries_commute`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___index_entries_commute): An index partition holds a set of `(value, primary key)` entries, so maintaining it is `setAdd`, not an append: two concurrent inserts of different entries commute and neither is lost (`setAdd_comm`, `setAdd_idem`).
- [`setRemove_not_comm_with_add`](Ferrosa/Cql/Index/#Ferrosa___Cql___Index___setRemove_not_comm_with_add): A removal does not commute with an add of the same value: adding then removing clears the entry, while removing then adding leaves it present.

## `Ferrosa.Cql.Subscribe`

[The module page](Ferrosa/Cql/Subscribe/)

- [`the_default_subscribe_mode_is_a_polled_snapshot`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___the_default_subscribe_mode_is_a_polled_snapshot): A `SUBSCRIBE` that names no `ON` clause polls a snapshot once a second, so a client that says nothing gets the mode that cannot lose an event to a lagging stream.
- [`snapshot_polling_cannot_be_tighter_than_half_a_second`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___snapshot_polling_cannot_be_tighter_than_half_a_second): A client cannot ask for sub-half-second polling, so the write amplification of snapshot mode is bounded below by the protocol rather than by client restraint.
- [`a_snapshot_misses_what_a_stream_delivers`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___a_snapshot_misses_what_a_stream_delivers): A snapshot can miss an intermediate state; a change stream cannot.
- [`on_local_and_on_committed_select_different_streams`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___on_local_and_on_committed_select_different_streams): `ON` accepts `LOCAL` and/or `COMMITTED`, and the two select different streams.
- [`only_the_stream_modes_can_report_a_gap`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___only_the_stream_modes_can_report_a_gap): Only the two change-stream shapes can report a gap: a snapshot re-reads instead, so falling behind costs it nothing it has to tell the client about.
- [`cql_subscribe_has_no_red_items`](Ferrosa/Cql/Subscribe/#Ferrosa___Cql___Subscribe___cql_subscribe_has_no_red_items): All five `SUBSCRIBE` obligations in the ledger are discharged, so none of them is a gap a client has to be warned about.

## `Ferrosa.Sql.Semantics`

[The module page](Ferrosa/Sql/Semantics/)

- [`null_passes_a_naive_not_equal`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___null_passes_a_naive_not_equal): **Red.** A NULL row passes the naive predicate and fails the standard one.
- [`the_two_agree_without_nulls`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___the_two_agree_without_nulls): The same divergence on a non-null value: the naive translation is right whenever no NULL is involved, which is why the bug survives testing.
- [`commit_is_not_atomic_across_statements`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___commit_is_not_atomic_across_statements): **Red.** `COMMIT` is not atomic across statements.
- [`status_is_not_a_durability_claim`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___status_is_not_a_durability_claim): Reporting `InTransaction` does not make it so: the status the client sees is a function of the session, not of what is durable.
- [`sum_of_no_rows_is_null_not_zero`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___sum_of_no_rows_is_null_not_zero): **Red.** `SUM` over no rows is NULL, not 0.
- [`sums_agree_when_there_are_rows`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___sums_agree_when_there_are_rows): On non-empty input they agree, which is why this survives a test suite.
- [`count_star_and_count_col_disagree`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___count_star_and_count_col_disagree): `COUNT(*)` counts rows, `COUNT(col)` skips NULL.
- [`sequence_need_only_be_monotone`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___sequence_need_only_be_monotone): A gap is legal, so a sequence that consumed an id on a lost transaction — or that replayed a counter mutation — is still a correct sequence.
- [`a_sequence_must_not_decrease`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___a_sequence_must_not_decrease): And a sequence that goes backwards is not: this is the property to hold.
- [`sql_has_three_red_items`](Ferrosa/Sql/Semantics/#Ferrosa___Sql___sql_has_three_red_items): Three of the six obligations the SQL surface carries are red: multi-statement atomicity, three-valued logic, and `SUM` over the empty set.

## `Ferrosa.Sql.Index`

[The module page](Ferrosa/Sql/Index/)

- [`equality_contacts_one_node`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___equality_contacts_one_node): An equality lookup touches exactly one node.
- [`range_lookup_cannot_be_constant`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___range_lookup_cannot_be_constant): A range lookup cannot be constant.
- [`only_equality_is_constant`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___only_equality_is_constant): The honest bound: equality is O(1) in nodes, range is O(nodes).
- [`index_lookup_is_constant`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___index_lookup_is_constant): An index probe reads exactly one record whatever the table holds, which is the whole of the constant-time claim.
- [`index_lookup_beats_a_scan`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___index_lookup_beats_a_scan): An index probe beats a scan exactly when there is more than one row, which is the whole claim: the cost does not grow with the table.
- [`a_range_probe_is_not_constant`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___a_range_probe_is_not_constant): A range probe reads every matching row, so it grows with the number of matches and is never a constant-cost operation.
- [`a_non_atomic_index_write_is_inconsistent`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___a_non_atomic_index_write_is_inconsistent): **Red.** One-sided maintenance is inconsistent immediately, in both directions: a row with no entry is invisible to the index (a lost lookup), an entry with no row is a lookup that fails after finding it (a dangling pointer).
- [`atomic_maintenance_preserves_consistency`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___atomic_maintenance_preserves_consistency): Maintaining both sides in one step keeps the invariant for any starting state that had it.
- [`scatter_gather_is_the_accepted_shape`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___scatter_gather_is_the_accepted_shape): Scatter-gather is the accepted shape: `nodes` hops for `nodes × rowsPerNode` rows, so with a small node count and a large match the hops are noise and the rows dominate.
- [`sql_index_has_one_red_item`](Ferrosa/Sql/Index/#Ferrosa___Sql___Index___sql_index_has_one_red_item): One of the five index obligations is red: the index and the table can diverge, because maintenance today runs as a separate step rather than inside the row write.

## `Ferrosa.Sql.Txn`

[The module page](Ferrosa/Sql/Txn/)

- [`independent_execution_is_not_atomic`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___independent_execution_is_not_atomic): Independent execution: each statement commits on its own, so a crash leaves a prefix.
- [`one_decision_is_atomic`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___one_decision_is_atomic): A single commit decision leaves either everything applied or nothing, whichever way it went: there is no third outcome for the participants to disagree about.
- [`acting_early_can_contradict_the_decision`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___acting_early_can_contradict_the_decision): A participant that acts before the decision is known can diverge from it, which is why the decision must be durable before any participant applies it.
- [`snapshot_isolation_permits_write_skew`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___snapshot_isolation_permits_write_skew): Snapshot isolation permits write skew.
- [`serializable_requires_the_read_set`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___serializable_requires_the_read_set): **Red.** The isolation level is the price.
- [`a_commit_acked_before_sync_can_be_lost`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___a_commit_acked_before_sync_can_be_lost): Durability is the commitlog premise, moved to the decision.
- [`txn_has_two_red_items`](Ferrosa/Sql/Txn/#Ferrosa___Sql___Txn___txn_has_two_red_items): Two of the six transaction obligations are red: a commit record durable before the acknowledgement, and serializable isolation.

## `Ferrosa.Sql.Consistency`

[The module page](Ferrosa/Sql/Consistency/)

- [`default_is_quorum`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___default_is_quorum): A statement or transaction that names no level runs at `QUORUM`, which on three replicas is two of them — enough to meet another `QUORUM` read and not enough to survive two failures.
- [`quorum_delegates_to_each_datacenter`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___quorum_delegates_to_each_datacenter): QUORUM delegates: three in the primary, two in the secondary.
- [`local_quorum_constrains_only_the_coordinating_dc`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___local_quorum_constrains_only_the_coordinating_dc): LOCAL_QUORUM constrains only the coordinating datacenter, and by the *local* replica count — three of five here, not a function of the eight total.
- [`all_requires_every_replica`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___all_requires_every_replica): `ALL` on a five-and-three keyspace requires all five replicas in the primary and all three in the secondary, so it tolerates no failure at all.
- [`one_is_not_a_per_dc_requirement`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___one_is_not_a_per_dc_requirement): ONE is not a per-datacenter requirement at all: it is "one replica, anywhere".
- [`a_total_count_can_hide_a_missing_datacenter`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_total_count_can_hide_a_missing_datacenter): **Red.** Five acks in the primary and none in the secondary satisfy the total and fail the requirement.
- [`total_rule_undershoots_on_an_even_split`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___total_rule_undershoots_on_an_even_split): The total rule and the per-DC rule agree on `{5,3}` and disagree on an even split.
- [`acceptance_table_agrees_with_the_rule`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___acceptance_table_agrees_with_the_rule): The table is the rule.
- [`sql_levels_agree_with_the_topology_rule`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___sql_levels_agree_with_the_topology_rule): The SQL level set is not a second arithmetic.
- [`levels_trade_acknowledgements_for_failures`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___levels_trade_acknowledgements_for_failures): Cost rises with the level: fewer acknowledgements, more tolerated failures.
- [`quorum_reads_see_quorum_writes_rf3`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___quorum_reads_see_quorum_writes_rf3): QUORUM reads see QUORUM writes on RF=3.
- [`one_read_can_miss_a_one_write`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___one_read_can_miss_a_one_write): ONE reads need not see ONE writes.
- [`all_writes_are_seen_by_every_read`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___all_writes_are_seen_by_every_read): ALL writes survive every read level, which is the only configuration where a read at ONE is safe against a previous write.
- [`local_quorum_does_not_cross_dcs`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___local_quorum_does_not_cross_dcs): LOCAL_QUORUM does not cross datacenters.
- [`a_weak_decision_level_can_lose_the_verdict`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_weak_decision_level_can_lose_the_verdict): A commit decision acknowledged at ONE while the data is at QUORUM: if that one replica is lost, the verdict is gone and the data is not.
- [`quorum_and_each_quorum_agree_on_two_dcs`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___quorum_and_each_quorum_agree_on_two_dcs): On two DCs the two levels are the same, so `{5,3}` needs `[3, 2]` either way — and five acks in the primary with none in the secondary still fails.
- [`each_quorum_is_stricter_than_quorum_above_two_dcs`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___each_quorum_is_stricter_than_quorum_above_two_dcs): Above two DCs they diverge.
- [`a_partition_does_not_shrink_the_dc_denominator`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_partition_does_not_shrink_the_dc_denominator): A partition cannot move the denominator: with one DC dark, the requirement is still a majority of the three configured DCs, so two live DCs are needed, not one.
- [`the_origin_dc_gate_is_not_satisfied_by_remote_acks`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_origin_dc_gate_is_not_satisfied_by_remote_acks): And the origin gate is separate from the DC count: two remote DCs do not stand in for a primary that has not reached its own majority.
- [`pr_and_cassandra_quorum_are_incomparable`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___pr_and_cassandra_quorum_are_incomparable): Each rule accepts a write the other rejects, on the same `{3,3,3}` keyspace: `[5, 0, 0]` is five acks, which is Cassandra's quorum of nine, but only one of three DCs has its majority, so the shape rule rejects it.
- [`the_two_rules_agree_on_two_dcs`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_two_rules_agree_on_two_dcs): On two DCs they agree, which is why the divergence only appears on three or more.
- [`the_multi_dc_default_is_dc_quorum`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_multi_dc_default_is_dc_quorum): A multi-DC keyspace defaults to DC_QUORUM, a single-DC one to Cassandra's QUORUM.
- [`dc_quorum_collapses_to_quorum_on_one_dc`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___dc_quorum_collapses_to_quorum_on_one_dc): On one DC with RF 3 the two rules give the same verdict at every acknowledgement count, so the mode adds nothing until there is a second datacenter to require.
- [`dc_quorum_trades_total_durability_for_dc_shape`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___dc_quorum_trades_total_durability_for_dc_shape): What the default trades away: DC_QUORUM guarantees a *shape* — a majority of DCs, each with a majority of its own replicas — and not Cassandra's *total*.
- [`requiring_both_rejects_both_bad_shapes`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___requiring_both_rejects_both_bad_shapes): Requiring a datacenter quorum and a node quorum together refuses both bad shapes on `{3,3,3}`: five acks in one datacenter fail the shape, and two datacenters' majorities of four acks fail Cassandra's total of five.
- [`a_zero_rf_datacenter_is_not_in_the_denominator`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_zero_rf_datacenter_is_not_in_the_denominator): A datacenter configured with `rf = 0` is not in the denominator.
- [`dc_quorum_also_needs_a_node_quorum`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___dc_quorum_also_needs_a_node_quorum): `{5,5,5}` accepts `{5,3,0}` and rejects `{3,3,0}`.
- [`dc_quorum_tolerates_a_whole_dc_outage`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___dc_quorum_tolerates_a_whole_dc_outage): `{5,5,5}` keeps committing through a whole datacenter outage.
- [`the_survivors_must_supply_eight_of_ten`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_survivors_must_supply_eight_of_ten): And the window is exact.
- [`the_last_writer_is_the_one_told_to_retry`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_last_writer_is_the_one_told_to_retry): **Red.** The last writer is the one told to retry.
- [`a_retry_verdict_does_not_modify_the_state`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_retry_verdict_does_not_modify_the_state): A retry verdict is not a write.
- [`a_rejected_writer_keeps_the_invariant`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_rejected_writer_keeps_the_invariant): And rejecting preserves the invariant the pair would otherwise break.
- [`the_conflict_is_named_for_the_client`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_conflict_is_named_for_the_client): The conflict is named, so the client can re-read exactly the rows that collided and decide for itself whether to retry.
- [`only_a_reread_changes_the_verdict`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___only_a_reread_changes_the_verdict): A retry must re-read.
- [`local_quorum_ack_is_blind_to_the_other_datacenters`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___local_quorum_ack_is_blind_to_the_other_datacenters): Strong inside the datacenter, silent about everywhere else.
- [`a_failover_can_lose_unboundedly_many_local_acks`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_failover_can_lose_unboundedly_many_local_acks): And the window has no bound.
- [`a_node_quorum_cannot_be_split`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_node_quorum_cannot_be_split): Any two node majorities intersect, so a write acknowledged by a majority is still found by any later quorum read or repair.
- [`a_local_quorum_can_sit_outside_every_future_quorum`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_local_quorum_can_sit_outside_every_future_quorum): And that is exactly what LOCAL_QUORUM does not buy.
- [`the_level_is_carried_not_ranked`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_level_is_carried_not_ranked): The decision level is carried by the transaction, never ranked from the enum.
- [`an_upgraded_commit_is_exactly_as_durable_as_its_data`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___an_upgraded_commit_is_exactly_as_durable_as_its_data): Committing at the requested level makes the verdict exactly as durable as the data it commits: both are written at the transaction's level, so the verdict can never be the weaker of the two.
- [`riding_the_last_mutation_is_safe_once_upgraded`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___riding_the_last_mutation_is_safe_once_upgraded): And the carrier stops mattering: whichever statement rides the commit record, it is written at the transaction's level, so riding the last mutation is safe once the data is upgraded.
- [`quorum_is_cassandras_total_rule`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___quorum_is_cassandras_total_rule): Cassandra's QUORUM is the total rule alone — `block_for` returns `rf / 2 + 1` on the total replication factor, and that is all it is.
- [`quorum_is_strictly_weaker_than_dc_quorum`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___quorum_is_strictly_weaker_than_dc_quorum): DC_QUORUM is strictly stronger than Cassandra's QUORUM: it adds the datacenter shape, and `[5, 0, 0]` on `{3,3,3}` is a write QUORUM accepts (five of nine) and DC_QUORUM refuses (one datacenter of three).
- [`local_quorum_alone_does_not_prevent_a_split`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___local_quorum_alone_does_not_prevent_a_split): Local quorum alone does not prevent a split.
- [`two_disjoint_dc_sets_cannot_both_hold_a_majority`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___two_disjoint_dc_sets_cannot_both_hold_a_majority): The datacenter-majority condition is what prevents it: two disjoint sets of datacenters cannot both contain a strict majority of the configured ones.
- [`dc_quorum_writes_need_both_conditions`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___dc_quorum_writes_need_both_conditions): A write needs both conditions.
- [`a_local_read_does_not_consult_remote_datacenters`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_local_read_does_not_consult_remote_datacenters): A local read is blind to remote replication.
- [`a_failing_over_client_may_not_read_its_own_write`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_failing_over_client_may_not_read_its_own_write): So a client that fails over between datacenters can read backwards.
- [`local_quorum_commits_have_a_datacenter_failure_rpo`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___local_quorum_commits_have_a_datacenter_failure_rpo): A LOCAL_QUORUM commit buys a datacenter-failure RPO, not cross-datacenter durability.
- [`a_local_read_sees_a_dc_quorum_write`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_local_read_sees_a_dc_quorum_write): And the local-read guarantee follows from the write rule: any write that satisfied DC_QUORUM left its origin datacenter at quorum, so reading there needs no hop.
- [`a_decision_at_the_weakest_statement_is_not_enough`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_decision_at_the_weakest_statement_is_not_enough): Deciding at the weakest statement's level is not enough.
- [`a_decision_at_the_strongest_statement_covers_every_one`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___a_decision_at_the_strongest_statement_covers_every_one): Deciding at the strongest covers every statement, which is the rule.
- [`levels_are_not_ordered_independently_of_the_configuration`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___levels_are_not_ordered_independently_of_the_configuration): But "strongest" cannot be an enum order.
- [`riding_the_last_mutation_is_not_enough_when_it_is_the_weakest`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___riding_the_last_mutation_is_not_enough_when_it_is_the_weakest): Riding the last mutation is not enough when the last mutation is the weakest.
- [`the_carrier_must_be_the_strongest_statement`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___the_carrier_must_be_the_strongest_statement): So the carrier must be the transaction's strongest statement.
- [`no_window_when_the_decision_rides_the_data`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___no_window_when_the_decision_rides_the_data): And then there is no window at all, because the verdict is not a second write: on a one-statement transaction the durability of the decision *is* the durability of the data.
- [`no_consistency_level_prevents_write_skew`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___no_consistency_level_prevents_write_skew): No acknowledgement threshold can prevent write skew.
- [`read_set_validation_is_what_prevents_write_skew`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___read_set_validation_is_what_prevents_write_skew): Read-set validation is what prevents it.
- [`all_does_not_prevent_write_skew`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___all_does_not_prevent_write_skew): No consistency level prevents write skew.
- [`consistency_has_two_red_items`](Ferrosa/Sql/Consistency/#Ferrosa___Sql___Consistency___consistency_has_two_red_items): Two of the ledger's obligations are red: one write path enforces a total rather than a per-datacenter requirement, and no read set is captured or validated before commit, so a write skew commits with no exception raised.

## `Ferrosa.Sparql.Semantics`

[The module page](Ferrosa/Sparql/Semantics/)

- [`a_duplicated_triple_double_counts`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___a_duplicated_triple_double_counts): A duplicated triple double-counts.
- [`adding_a_triple_never_removes_a_match`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___adding_a_triple_never_removes_a_match): Adding a triple never removes a solution.
- [`deleting_a_triple_can_remove_a_match`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___deleting_a_triple_can_remove_a_match): And the converse fails for deletion: removing a triple can remove a match that a duplicate elsewhere still justified is no longer justified.
- [`closure_without_a_visited_set_duplicates_pairs`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___closure_without_a_visited_set_duplicates_pairs): **Red.** `p+` must return each pair once.
- [`a_self_loop_diverges_without_fuel`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___a_self_loop_diverges_without_fuel): The same for a self-loop, where an expansion without memory never terminates at all given unbounded fuel — the fuel bound is the only thing stopping it, which is not a semantics.
- [`encoding_is_not_injective`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___encoding_is_not_injective): **Red.** A quoted triple aliases with an IRI.
- [`two_terms_share_a_store_key`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___two_terms_share_a_store_key): The consequence, stated on the store: one key, two distinct terms.
- [`sparql_has_three_red_items`](Ferrosa/Sparql/Semantics/#Ferrosa___Sparql___sparql_has_three_red_items): Three of the six SPARQL obligations are red: `p+` returns a row per path rather than per pair, a quoted triple can share a store key with an IRI, and `OPTIONAL` scoping has no representation at all.

## `Ferrosa.Bolt.Session`

[The module page](Ferrosa/Bolt/Session/)

- [`failure_is_sticky`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___failure_is_sticky): Only RESET (or GOODBYE) leaves the failed state.
- [`reset_clears_streams_and_failure`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___reset_clears_streams_and_failure): RESET restores the initial state exactly: no queued stream survives it, and a failed session becomes usable again.
- [`replies_are_not_one_per_message`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___replies_are_not_one_per_message): **Red.** Replies are not one per message.
- [`ignored_messages_shift_every_later_reply`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___ignored_messages_shift_every_later_reply): **Red.** The desynchronisation is cumulative.
- [`unconsumed_runs_grow_the_queue`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___unconsumed_runs_grow_the_queue): **Red.** RUN without PULL accumulates.
- [`pull_and_discard_both_release`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___pull_and_discard_both_release): PULL consumes; DISCARD also consumes.
- [`bolt_has_three_red_items`](Ferrosa/Bolt/Session/#Ferrosa___Bolt___bolt_has_three_red_items): Three of the six Bolt obligations are red: replies cannot be paired to messages by position, queued result streams are unbounded, and a failure carries no statement identity for the client to recover from.
-/
