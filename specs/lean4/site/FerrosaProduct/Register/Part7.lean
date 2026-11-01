/-!
# Register VII: Limits and accounting

Not everything here is proved, and the corpus is built so that the unproved part cannot be guessed at. Every red item in every ledger is classified exactly once — unimplemented, requirement, proof, measurement, inherited, undecided — and the counts are pinned by theorems. This part is the audit: the classification, the trusted base under every `unsafe` block, and the items still owed.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Unimplemented`

[The module page](Ferrosa/Unimplemented/)

- [`cql_aggregate_bridge_preserves_reds`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___cql_aggregate_bridge_preserves_reds): The bridge does not change the count: the six the module pins for itself are the six counted here.
- [`findLedger_mem`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___findLedger_mem): A `find?` result is a list element: the registry cannot answer with a ledger it does not hold.
- [`area_keys_are_distinct`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___area_keys_are_distinct): Registering one ledger twice would double-count it, so the registry is a set of keys.
- [`registered_ledger_count`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___registered_ledger_count): Pinned so that dropping a ledger with no reds — which the total cannot see — is still a build failure rather than a quietly smaller registry.
- [`ledgerTotal_is_the_registry_sum`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___ledgerTotal_is_the_registry_sum): The corpus total is by definition the sum of the registered ledgers' red counts; there is no literal term anywhere that could fall out of step with the ledgers it counts.
- [`ledgerTotal_pinned`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___ledgerTotal_pinned): Pinned so that removing a ledger from the registry — the drift that has recurred here — is a build failure rather than a silently smaller total.
- [`resolves_spec`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___resolves_spec): A `resolves` hit is a real row of a real registered ledger — the witness form of the same fact, so a failure names the item instead of a boolean.
- [`every_classified_row_is_red`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___every_classified_row_is_red): (a) Every classification row names a ledger item, and that item is red.
- [`every_row_names_a_red_item`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___every_row_names_a_red_item): (a), in witness form.
- [`every_red_is_classified_exactly_once`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___every_red_is_classified_exactly_once): (b) Every red item of every registered ledger is named by exactly one row: no red is unclassified, and none is classified twice.
- [`per_area_counts`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___per_area_counts): (c) Each area's rows number exactly that area's reds.
- [`every_discharged_row_names_an_item`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___every_discharged_row_names_an_item): A discharged row still names a real ledger item, so a row cannot rot into a dangling string.
- [`discharged_rows_name_met_items`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___discharged_rows_name_met_items): …and the item it names is `.met`: a row that names a *red* item is in the wrong list, and one that names nothing at all fails above.
- [`nothing_discharged_is_still_classified`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___nothing_discharged_is_still_classified): A discharged row is not also a classification row, so the partition cannot hide a red here.
- [`rows_are_the_ledger_reds`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___rows_are_the_ledger_reds): The totals agree, which is now a consequence of (a), (b) and (c) rather than a coincidence.
- [`every_ledger_item_is_classified`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___every_ledger_item_is_classified): Every red item in every ledger appears here or in `Broken.broken`, exactly once.
- [`unimplemented_has_the_right_length`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___unimplemented_has_the_right_length): Thirty-nine items have no code path yet, and the count is pinned so a row cannot disappear between commits without this failing.
- [`twelve_are_not_defects`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___twelve_are_not_defects): Seven, not eight: two rows were discharged into the ledger itself (see `discharged`), and one new row records the refuted constant-probe claim in the MVCC ledger.
- [`live_defects_outnumber_the_rest`](Ferrosa/Unimplemented/#Ferrosa___Unimplemented___live_defects_outnumber_the_rest): Live defects now outnumber everything else.

## `Ferrosa.Broken`

[The module page](Ferrosa/Broken/)

- [`broken_has_sixteen_items`](Ferrosa/Broken/#Ferrosa___Broken___broken_has_sixteen_items): Sixteen built-and-wrong items, pinned so a defect cannot be dropped from the list without the corpus classification breaking.
- [`the_surfaces_carry_five`](Ferrosa/Broken/#Ferrosa___Broken___the_surfaces_carry_five): The surfaces are where the damage concentrates: five of the sixteen are SQL or SPARQL, which are the two places a client is most likely to be told something confidently wrong.
- [`five_rows_have_no_verified_site`](Ferrosa/Broken/#Ferrosa___Broken___five_rows_have_no_verified_site): Five of the sixteen could not be anchored to the shipping code: four describe an implementation that does the correct thing at this revision (with tests pinning it) and one is stale because the fix it says is unmerged…
- [`eleven_rows_name_a_site`](Ferrosa/Broken/#Ferrosa___Broken___eleven_rows_name_a_site): The remainder — the rows that name a real location — are what the list's opening claim is about: code that was read, not inferred.
- [`every_row_reads_as_prose`](Ferrosa/Broken/#Ferrosa___Broken___every_row_reads_as_prose): Every row says what is wrong in words a product reader can follow.
- [`model_only_rows_say_so`](Ferrosa/Broken/#Ferrosa___Broken___model_only_rows_say_so): A reader is told when a row is about the model, not the shipping code.

## `Ferrosa.Unsafe.Invariants`

[The module page](Ferrosa/Unsafe/Invariants/)

- [`two_mutable_borrows_cannot_coexist`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___two_mutable_borrows_cannot_coexist): No two mutable borrows of the same cell.
- [`a_mutable_borrow_excludes_readers`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_mutable_borrow_excludes_readers): A mutable borrow excludes readers, and readers exclude a mutable borrow.
- [`shared_borrows_stack`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___shared_borrows_stack): Shared borrows stack, which is what `&self` methods need.
- [`a_shared_borrow_racing_a_mutation_is_the_promised_race`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_shared_borrow_racing_a_mutation_is_the_promised_race): `unsafe impl Sync` is justified only if mutation requires exclusivity.
- [`a_reference_used_past_its_owner_is_invalid`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_reference_used_past_its_owner_is_invalid): **Red.** Using a reference after its owner is gone is the violation.
- [`a_static_lifetime_does_not_extend_the_region`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_static_lifetime_does_not_extend_the_region): Extending a lifetime does not extend the region: `&'static T` from a shorter-lived owner is sound only if no use occurs after the owner dies.
- [`a_truncation_below_a_mapping_makes_the_tail_unreadable`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_truncation_below_a_mapping_makes_the_tail_unreadable): **Red.** Truncating a mapped file below an offset makes that offset unreadable, so the mapping's soundness is a claim about a different subsystem: nothing may truncate while a mapping exists.
- [`a_slice_outliving_its_mapping_is_a_dangling_read`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_slice_outliving_its_mapping_is_a_dangling_read): And the mapping must outlive its own slices: dropping the `Mmap` while a slice derived from it is live is a dangling read, exactly like case 2.
- [`a_block_freed_with_its_own_layout_is_balanced`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_block_freed_with_its_own_layout_is_balanced): The round trip is balanced.
- [`a_block_freed_with_a_different_layout_corrupts_the_heap`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_block_freed_with_a_different_layout_corrupts_the_heap): **Red.** A layout mismatch is heap corruption, not a leak: freeing with a larger size hands the allocator a block boundary it will reuse for something else.
- [`a_failed_syscall_has_no_out_parameter`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_failed_syscall_has_no_out_parameter): A failed syscall has no out-parameter.
- [`checking_the_return_code_is_what_makes_the_read_defined`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___checking_the_return_code_is_what_makes_the_read_defined): An out-parameter is readable exactly when the call succeeded, so branching on the return code before touching the buffer is what makes the read defined rather than undefined.
- [`overlapping_writers_are_the_race`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___overlapping_writers_are_the_race): Two writers on overlapping intervals are the race the marker impl denies.
- [`overlapping_readers_are_fine`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___overlapping_readers_are_fine): Two readers may overlap, which is what makes `&self` concurrency possible at all.
- [`a_disjoint_writer_and_reader_are_fine`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_disjoint_writer_and_reader_are_fine): A writer disjoint from a reader is fine.
- [`a_handle_from_a_live_context_is_usable`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_handle_from_a_live_context_is_usable): A handle from a live context is usable.
- [`a_handle_after_its_context_is_dropped_is_dangling`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___a_handle_after_its_context_is_dropped_is_dangling): A handle used after its context is dropped is dangling.
- [`unsafe_has_three_red_items`](Ferrosa/Unsafe/Invariants/#Ferrosa___Unsafe___unsafe_has_three_red_items): Three reds, and between them they are the trusted base's whole gap: no Kani integration, the truncation contract stated but not enforced, and no separation-logic route to `unsafe`.

## `Ferrosa.Todo`

[The module page](Ferrosa/Todo/)

- [`the_unspecified_surfaces_are_ten`](Ferrosa/Todo/#Ferrosa___Todo___the_unspecified_surfaces_are_ten): The manifest is pinned: an entry leaves only when its module arrives.
-/
