/-!
# Register IV: The write path

Now the inside. A write is admitted, appended to the commit log, replicated to the sync-mode target, applied to the memtable, acknowledged, and only then flushed to an SSTable. Each arrow in that sentence is a constructor of `Step` in `WritePath`, which is why the trace can be checked rather than described. The storage modules then take the path apart: the cell format, the visibility rule, what survives a crash, and what a delete leaves behind.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.WritePath`

[The module page](Ferrosa/WritePath/)

- [`log_append_only`](Ferrosa/WritePath/#Ferrosa___WritePath___log_append_only): The commit log only ever grows.
- [`step_synced_le`](Ferrosa/WritePath/#Ferrosa___WritePath___step_synced_le): Step-level: the durable prefix never runs past the log.
- [`synced_le_logged`](Ferrosa/WritePath/#Ferrosa___WritePath___synced_le_logged): The fsynced prefix never runs past the log, so the durable region is always a real prefix of what was written — the premise every durability statement about a reachable store is stated against.
- [`step_acked_le`](Ferrosa/WritePath/#Ferrosa___WritePath___step_acked_le): Step-level: the ack counter never outruns the log.
- [`acked_le_logged`](Ferrosa/WritePath/#Ferrosa___WritePath___acked_le_logged): No ack without a log record.
- [`step_batch_durable`](Ferrosa/WritePath/#Ferrosa___WritePath___step_batch_durable): Step-level: under `Batch`, acks are always durable.
- [`batch_ack_is_durable`](Ferrosa/WritePath/#Ferrosa___WritePath___batch_ack_is_durable): Batch mode closes the loss window: every acked write is fsynced.
- [`periodic_ack_can_be_lost`](Ferrosa/WritePath/#Ferrosa___WritePath___periodic_ack_can_be_lost): FMEA ST-1, proved reachable.
- [`admitted_sticky`](Ferrosa/WritePath/#Ferrosa___WritePath___admitted_sticky): Admission failure cannot be undone: no transition flips `admitted` back.
- [`no_append_after_admission_failure`](Ferrosa/WritePath/#Ferrosa___WritePath___no_append_after_admission_failure): Fail-closed, precisely: once admission has failed, no subsequent transition appends to the commit log.
- [`active_subset_log`](Ferrosa/WritePath/#Ferrosa___WritePath___active_subset_log): Every memtable row is in the commit log.
- [`flush_keeps_rows`](Ferrosa/WritePath/#Ferrosa___WritePath___flush_keeps_rows): Flush loses no row: the seal-then-publish sequence carries every row of the active memtable into the published SSTable.
- [`quarantine_retains_row`](Ferrosa/WritePath/#Ferrosa___WritePath___quarantine_retains_row): Quarantine retains the rejected row in the durable sidecar.
- [`quarantine_keeps_others`](Ferrosa/WritePath/#Ferrosa___WritePath___quarantine_keeps_others): Quarantine leaves every *other* row in the memtable.
- [`resolveCell_cons`](Ferrosa/WritePath/#Ferrosa___WritePath___resolveCell_cons): Resolution is defined one cell at a time: the answer for a newest-first list is that list's head merged into the answer for its tail, which is the step form every read-path argument uses.
- [`mergeCell_ne_none`](Ferrosa/WritePath/#Ferrosa___WritePath___mergeCell_ne_none): A merge always yields a cell once it has an incoming one, whatever was there before, so a partition's resolution cannot be empty by accident.
- [`resolveCell_none`](Ferrosa/WritePath/#Ferrosa___WritePath___resolveCell_none): So resolution returns nothing only for the empty list: a partition with a cell always resolves to a value, and a missing answer means there was no cell there rather than a lost one.
- [`mergeCell_monotone`](Ferrosa/WritePath/#Ferrosa___WritePath___mergeCell_monotone): LWW never rewinds a cell's timestamp.
- [`mergeCell_idempotent`](Ferrosa/WritePath/#Ferrosa___WritePath___mergeCell_idempotent): Idempotent apply — the property Accord's applied-txn ledger (I-28) relies on: replaying the same write twice changes nothing.
- [`resolveCell_mem`](Ferrosa/WritePath/#Ferrosa___WritePath___resolveCell_mem): Resolution returns an element of the list.
- [`resolveCell_max`](Ferrosa/WritePath/#Ferrosa___WritePath___resolveCell_max): Resolution returns a maximum-timestamp cell.
- [`resolveCell_sees_newest`](Ferrosa/WritePath/#Ferrosa___WritePath___resolveCell_sees_newest): A newer write is immediately visible to a later read.
- [`lww_ties_are_order_dependent`](Ferrosa/WritePath/#Ferrosa___WritePath___lww_ties_are_order_dependent): Equal timestamps are order-dependent.
- [`ok_means_threshold_met`](Ferrosa/WritePath/#Ferrosa___WritePath___ok_means_threshold_met): Invariant 1, forward direction: a returned `Ok` really does mean the threshold was met.
- [`short_ack_set_must_fail`](Ferrosa/WritePath/#Ferrosa___WritePath___short_ack_set_must_fail): Invariant 1, reverse direction: when the live replica count is short the coordinator cannot return `Ok`, so the only alternative is `Unavailable` — never a stale success.
- [`acks_are_replicas`](Ferrosa/WritePath/#Ferrosa___WritePath___acks_are_replicas): Acks are always drawn from the replica set.
- [`quorum_read_sees_quorum_write`](Ferrosa/WritePath/#Ferrosa___WritePath___quorum_read_sees_quorum_write): The write/read intersection requirement.

## `Ferrosa.Storage.Format`

[The module page](Ferrosa/Storage/Format/)

- [`short_record_is_not_verified`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___short_record_is_not_verified): A truncated record is not verified: the declared length overruns what is on disk.
- [`corrupt_record_is_not_verified`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___corrupt_record_is_not_verified): A record whose CRC mismatches is not verified.
- [`scan_is_a_prefix`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___scan_is_a_prefix): A torn file yields a prefix.
- [`corruption_stops_the_prefix`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___corruption_stops_the_prefix): One bad record costs one record.
- [`corruption_stops_the_scan`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___corruption_stops_the_scan): The scan in the form a reader of the table sees it: with every record before `bad` verified, the partitions recovered are exactly those records, and nothing the file holds after the damage is decoded as a partition.
- [`scan_finds_every_verified_frame`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___scan_finds_every_verified_frame): The data is self-describing.
- [`nthFrame_mem`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___nthFrame_mem): A record read at an offset is a record the file actually holds, so an offset a damaged index names can only ever reach real data.
- [`trie_cannot_produce_a_wrong_value`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___trie_cannot_produce_a_wrong_value): A wrong trie cannot produce a wrong value.
- [`trie_answer_agrees_with_the_scan`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___trie_answer_agrees_with_the_scan): A miss is recoverable.
- [`trie_reads_at_most_one_record`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___trie_reads_at_most_one_record): The reason the trie stays.
- [`rebuild_reads_every_record`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___rebuild_reads_every_record): Rebuilding from the data reads every record — which is correct, and why it is the fallback and not the fast path.
- [`corrupt_trie_block_is_detected`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___corrupt_trie_block_is_detected): A trie block whose checksum disagrees with its bytes fails its own check, so a damaged index is refused at the moment it is first touched rather than trusted for a lookup.
- [`absent_footer_is_recoverable`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___absent_footer_is_recoverable): An absent footer is a state, not a failure.
- [`count_mismatch_downgrades_to_recoverable`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___count_mismatch_downgrades_to_recoverable): A count mismatch is detected.
- [`both_servable`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___both_servable): A complete file is servable, and so is a recovered prefix.
- [`torn_is_not_servable`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___torn_is_not_servable): A bad header is the one case that is not servable — and it is quarantined, never deleted (`Storage.Recovery.only_quarantine_preserves_data`).
- [`random_access_touches_one_cell`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___random_access_touches_one_cell): A projection is one deref per projected column.
- [`sequential_access_touches_every_preceding_cell`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___sequential_access_touches_every_preceding_cell): The delta-encoded layout touches every cell up to `n`, which is why a narrow projection out of a wide row is expensive today.
- [`projection_beats_sequential_for_wide_rows`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___projection_beats_sequential_for_wide_rows): The gap is not a constant factor, it is the difference between one and `n`.
- [`collectAt_sound`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___collectAt_sound): Nothing is invented.
- [`collectAt_complete`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___collectAt_complete): Nothing is lost either.
- [`i32_ldt_wraps_where_u64_does_not`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___i32_ldt_wraps_where_u64_does_not): `fnf` stores the local deletion time as an unsigned 64-bit second count, so the `i32` wrap that `Storage.Tombstone.ldt_overflows_before_2038_reachable_now` exhibits cannot arise.
- [`encode_injective`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___encode_injective): The bytes determine the cell.
- [`meta_reads_do_not_touch_the_arena`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___meta_reads_do_not_touch_the_arena): Metadata reads never touch the value arena.
- [`popcountUpTo_step`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___popcountUpTo_step): The exception slot of cell `n + 1` is the slot of cell `n` plus one exactly when cell `n` is an exception — the step that turns the slot lookup into a single popcount.
- [`popcountUpTo_le_succ`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___popcountUpTo_le_succ): The slot index after `n + 1` cells is never below the index after `n`: walking one more cell can hand out a slot but never take one back, which is the induction step for the slot map being order-preserving.
- [`popcountUpTo_monotone`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___popcountUpTo_monotone): The slot map is order-preserving, so exception slots are handed out in cell order and two exception cells can never land on the same slot.
- [`exception_slots_differ`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___exception_slots_differ): Concrete: cells 0 and 2 are the exceptions, and they get slots 0 and 1.
- [`two_tier_never_costs_more`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___two_tier_never_costs_more): Two tiers never cost more than one flat cell, and are strictly cheaper as soon as one cell takes its row's timestamp.
- [`bitmap_fits_one_word`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___bitmap_fits_one_word): A 32-cell row needs exactly one bitmap word, so the slot lookup is one popcount.
- [`zone_map_prune_is_sound`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___zone_map_prune_is_sound): Pruning is sound.
- [`row_lookup_is_bounded_by_the_group`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___row_lookup_is_bounded_by_the_group): A row lookup is bounded by its group, not by the partition.
- [`column_ids_survive_a_new_column`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___column_ids_survive_a_new_column): Adding a column id leaves every id already in the schema where it was, so a table written before the change is read by the same ids afterwards and no existing cell has to be rewritten.
- [`all_true_of_mem`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___all_true_of_mem): A check that holds of every element of a list holds of each one of them — the step that turns the whole-partition well-formedness test into a bound on a single cell.
- [`well_formed_bounds_every_value`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___well_formed_bounds_every_value): A partition that passes its well-formedness check has every `(offset, length)` inside its arena, so projecting a cell reads value bytes the record actually holds and no part of a read can run past the end of the parti…
- [`a_payload_can_contain_the_sync_word`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___a_payload_can_contain_the_sync_word): The sync word alone is not enough.
- [`resync_yields_only_verified_records`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___resync_yields_only_verified_records): A confident resync lands only on records that verify.
- [`resync_loses_less_than_stopping`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___resync_loses_less_than_stopping): Resynchronising loses strictly less.
- [`an_inline_cell_reads_no_arena_bytes`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___an_inline_cell_reads_no_arena_bytes): An inline cell reads no arena bytes, so a projection of narrow columns never decompresses the value block at all.
- [`a_wide_cell_reads_its_value_from_the_arena`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___a_wide_cell_reads_its_value_from_the_arena): And a non-inline cell reads exactly its value — the arena is still the right place for the wide values, it is just not the only one.
- [`ack_at_data_fsync_is_recoverable`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___ack_at_data_fsync_is_recoverable): Acknowledge at data fsync is safe.
- [`format_has_three_red_items`](Ferrosa/Storage/Format/#Ferrosa___Storage___Format___format_has_three_red_items): Three obligations in this area are recorded red — a flush with no resume path, a BTI-to-Cassandra export, and the 5% CRC budget, which is a measurement rather than a theorem.

## `Ferrosa.Storage.Mvcc`

[The module page](Ferrosa/Storage/Mvcc/)

- [`naive_gc_breaks_a_live_snapshot`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___naive_gc_breaks_a_live_snapshot): The naive rule destroys a version a live snapshot still needs.
- [`a_stalled_reader_pins_every_version_behind_it`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_stalled_reader_pins_every_version_behind_it): A snapshot that never closes pins every version behind it.
- [`safe_gc_preserves_live_snapshots`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___safe_gc_preserves_live_snapshots): The floor rule preserves every live snapshot.
- [`gc_never_grows_a_chain`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___gc_never_grows_a_chain): GC never lengthens a chain, so it cannot cause unbounded growth in the record.
- [`a_version_per_file_is_not_a_constant_read`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_version_per_file_is_not_a_constant_read): **Red.** One version per file is not a constant read.
- [`in_record_versions_keep_the_probe_constant`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___in_record_versions_keep_the_probe_constant): **Red.** With the chain inside the record, a lookup reads one record however many snapshots can still see an older version — this is the constant the design is bought with, and it holds by the shape of the read rather than by…
- [`the_chain_bound_is_one_per_live_snapshot`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_chain_bound_is_one_per_live_snapshot): The chain has to hold one version per live snapshot plus the floor, and that count is the only thing bounding how long a record's version list can get: retention follows the oldest reader, never the clock.
- [`spill_never_loses_a_version`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___spill_never_loses_a_version): Nothing is lost: spilling is a move, not a discard.
- [`spill_bounds_the_inline_chain`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___spill_bounds_the_inline_chain): The cap holds after spilling, which is the whole point: the record stops growing.
- [`spilled_versions_are_lost_if_compaction_drops_the_pointer`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___spilled_versions_are_lost_if_compaction_drops_the_pointer): **Red.** Compaction must carry the pointer forward.
- [`a_spilled_row_costs_one_extra_probe`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_spilled_row_costs_one_extra_probe): A row that has spilled costs two record reads where a row that has not costs one, so an overflowing chain is paid for by the rows that overflowed and by no others.
- [`a_boolean_dirty_flag_cannot_identify_the_victim`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_boolean_dirty_flag_cannot_identify_the_victim): A boolean dirty bit cannot name the victim.
- [`a_tag_table_is_the_read_sets_indexed_by_row`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_tag_table_is_the_read_sets_indexed_by_row): **Red.** The tag table is exactly the read sets of every transaction, indexed by row — the same information, stored the other way round, so detection is identical and the representation costs one entry per distinct row a live…
- [`clearing_a_tag_early_loses_the_conflict`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___clearing_a_tag_early_loses_the_conflict): Clearing a tag early loses the conflict, and the horizon is the same one version retention needs: while both readers are live the write-skew pair is caught, and once the tags go the pair commits.
- [`a_predicate_lock_never_blocks_a_writer`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_predicate_lock_never_blocks_a_writer): A predicate lock never blocks a writer.
- [`nothing_waits_so_no_deadlock_can_form`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___nothing_waits_so_no_deadlock_can_form): No reader-writer deadlock can form, because no writer ever waits for a reader.
- [`contention_costs_a_retry_not_a_stall`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___contention_costs_a_retry_not_a_stall): Contention costs a retry, not a stall: a writer tripping one tag is refused naming its reader, and one tripping two is refused naming both — immediately, in both cases.
- [`a_transaction_does_not_conflict_with_its_own_reads`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_transaction_does_not_conflict_with_its_own_reads): And a transaction never conflicts with its own reads: read-then-write inside one snapshot is not a conflict, so no tag a transaction takes can reject its own commit.
- [`a_partition_tag_is_sound_but_over_approximates`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_partition_tag_is_sound_but_over_approximates): Partition granularity is sound and over-approximates: every read that a write touches is caught, and so is a write to a key in the same partition that the reader never touched.
- [`a_version_dies_at_zero`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_version_dies_at_zero): A version dies exactly when its last holder releases — no mark phase, no reachability test.
- [`a_vacuum_step_is_one_decrement`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_vacuum_step_is_one_decrement): **Red.** A vacuum step is one decrement and one test.
- [`a_chain_is_acyclic`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_chain_is_acyclic): The version graph is acyclic, so reference counting alone is exact.
- [`the_last_holder_returns_the_memory`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_last_holder_returns_the_memory): Two holders of ts 5 free it on the second release, not before.
- [`a_release_past_zero_stops_at_zero`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_release_past_zero_stops_at_zero): And releasing more than the count cannot happen: the first release that reaches zero reports it and stops, so a double release is a `none`, never an underflow.
- [`a_snapshot_reference_keeps_the_version_alive`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_snapshot_reference_keeps_the_version_alive): Committing releases the head's reference, and a live snapshot keeps the version at one.
- [`arc_agrees_with_the_floor_rule`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___arc_agrees_with_the_floor_rule): ARC and the floor rule agree: the floor survives precisely because the oldest snapshot holds a reference to it.
- [`the_default_stalled_reader_timeout_is_thirty_seconds`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_default_stalled_reader_timeout_is_thirty_seconds): A reader that reports no progress for thirty seconds is killed, and that single number is the whole of the policy: how long a reader has lived plays no part in the decision.
- [`an_idle_reader_is_killed_at_the_timeout`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___an_idle_reader_is_killed_at_the_timeout): An idle reader is killed at the timeout, and one millisecond before it is not.
- [`a_progressing_reader_is_never_killed_for_age`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_progressing_reader_is_never_killed_for_age): Total age never kills a reader; only the absence of progress does, so an hour-long scan that keeps reporting progress is untouched.
- [`killing_a_reader_returns_its_memory`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___killing_a_reader_returns_its_memory): Killing a reader returns its memory: every count it held falls to zero and each version dies in place, as with any other release.
- [`a_killed_reader_cannot_resume_its_snapshot`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_killed_reader_cannot_resume_its_snapshot): A killed reader cannot resume its snapshot: its versions may already be gone, so the next statement must fail rather than silently read a newer state, which would be an isolation violation with no error to show for it.
- [`a_snapshot_read_must_consult_every_file_holding_the_key`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___a_snapshot_read_must_consult_every_file_holding_the_key): A snapshot read cannot stop at the first file that holds the key.
- [`the_lookup_cost_is_linear_in_the_files_of_the_view`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_lookup_cost_is_linear_in_the_files_of_the_view): The lookup is linear in the files of the view, and nothing prunes that fan-out today: there is no leveled structure, so every lookup walks `guard.sstables`.
- [`the_chain_makes_a_covered_snapshot_one_read`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_chain_makes_a_covered_snapshot_one_read): A snapshot inside the chain window costs one record read; one older than the chain's oldest retained version falls back to the file fan-out.
- [`the_chain_does_not_reduce_the_bloom_fan_out`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___the_chain_does_not_reduce_the_bloom_fan_out): The chain does not reduce the bloom fan-out: the record still has to be found, so every SSTable in the view is probed either way.
- [`mvcc_has_five_red_items`](Ferrosa/Storage/Mvcc/#Ferrosa___Storage___Mvcc___mvcc_has_five_red_items): Five obligations are recorded red: no vacuum, no predicate locks taken on the read path, one value per cell rather than an in-record chain, a spill pointer no compaction carries, and the claim that a snapshot lookup m…

## `Ferrosa.Storage.Recovery`

[The module page](Ferrosa/Storage/Recovery/)

- [`put_is_idempotent`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___put_is_idempotent): `put` and `del` are idempotent; `inc` is not.
- [`del_is_idempotent`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___del_is_idempotent): Deleting the same key twice leaves the same keyspace as deleting it once, so a replayed delete costs a client nothing even when the dedup key is missing.
- [`inc_is_not_idempotent`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___inc_is_not_idempotent): A counter bump applied twice is not a counter bump.
- [`no_checksum_detects_only_truncation`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___no_checksum_detects_only_truncation): **Red.** Length checks catch truncation but not corruption.
- [`truncation_is_detected`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___truncation_is_detected): A truncated record is detectable — the one case the format does handle.
- [`stop_replays_a_prefix`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___stop_replays_a_prefix): The stop policy replays a prefix of what was written.
- [`skip_does_not_replay_a_prefix`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___skip_does_not_replay_a_prefix): Skipping does not replay a prefix.
- [`acked_survives_crash`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___acked_survives_crash): Under `onAck` there is no schedule that loses an acknowledged write: the sync count is at least the acked index by construction of the mode.
- [`periodic_sync_loses_acked_writes`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___periodic_sync_loses_acked_writes): Under periodic sync there is.
- [`only_on_ack_implies_durability`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___only_on_ack_implies_durability): The mode decides the premise: only `onAck` discharges `AckAfterSync`.
- [`zero_id_is_not_deduped`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___zero_id_is_not_deduped): A zero id is always re-applied — the code says so explicitly, and this is the property that makes it matter.
- [`zero_id_replay_duplicates_a_nonidempotent_mutation`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___zero_id_replay_duplicates_a_nonidempotent_mutation): **Red.** A zero-id counter record double-applies.
- [`nonzero_id_is_deduped`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___nonzero_id_is_deduped): With a non-zero id the record is re-applied at most once.
- [`recovery_is_deterministic`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___recovery_is_deterministic): Replay depends on the value at the key and nothing else about the store.
- [`only_quarantine_preserves_data`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___only_quarantine_preserves_data): **Red.** Deleting a corrupt file destroys the only copy.
- [`the_ack_guarantee_is_a_configuration_not_a_law`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___the_ack_guarantee_is_a_configuration_not_a_law): The acknowledgement's durability is a configuration, not a law.
- [`recovery_has_three_red_items`](Ferrosa/Storage/Recovery/#Ferrosa___Storage___Recovery___recovery_has_three_red_items): Three obligations here are recorded red: a payload checksum, idempotent replay, and a quarantine action the engine actually models.

## `Ferrosa.Storage.Tombstone`

[The module page](Ferrosa/Storage/Tombstone/)

- [`merge_prefers_newer`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___merge_prefers_newer): The newer write wins.
- [`merge_tombstone_wins_tie`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___merge_tombstone_wins_tie): The tombstone wins a timestamp tie.
- [`merge_idempotent`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___merge_idempotent): Merging a cell with itself is that cell — the property replay depends on.
- [`mergeTsOnly_idempotent`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___mergeTsOnly_idempotent): Timestamp-only merge is still idempotent, so replay is not where the missing tie-break shows up: the failure appears between replicas, each of which keeps its own second argument and never converges with the other.
- [`ts_only_merge_does_not_converge`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___ts_only_merge_does_not_converge): **Red.** Without a value tie-break, two replicas keep different winners.
- [`value_wins_ignores_a_delete`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___value_wins_ignores_a_delete): Without the tombstone tie-break, a delete is silently ignored.
- [`tombstone_tiebreak_resolves_a_delete`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___tombstone_tiebreak_resolves_a_delete): The tombstone-aware merge resolves the same pair to the delete, in both orders.
- [`merge_converges_on_collisions`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___merge_converges_on_collisions): With both tie-breaks the merge is commutative on the concrete colliding cases, so replicas agree whichever order they merge in.
- [`expiry_depends_only_on_the_stored_ldt`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___expiry_depends_only_on_the_stored_ldt): The stored number is what decides, so a replica's clock never enters the answer: two readers at the same instant agree even if one clock is wrong, because neither recomputes `ldt`.
- [`ttl_expires_exactly_at_ldt`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___ttl_expires_exactly_at_ldt): The same cell read at two instants: visible before `ldt`, gone at it.
- [`past_timestamp_expires_on_arrival`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___past_timestamp_expires_on_arrival): A write with a past `USING TIMESTAMP` and a TTL is invisible on arrival.
- [`ldt_overflows_before_2038_reachable_now`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___ldt_overflows_before_2038_reachable_now): **Red.** The `i32` local-deletion-time wraps past 2038.
- [`no_purge_preserves_absence`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___no_purge_preserves_absence): With no purge, absence is monotone: a merged tombstone stays the winner against any value it dominates, so deleted data cannot come back.
- [`purge_before_propagation_resurrects`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___purge_before_propagation_resurrects): **Red.** Replica A purges its tombstone when grace elapses while replica B never saw the delete; read repair then pulls B's older value back and the row is visible again, with no tombstone remaining to hide it.
- [`purge_safe_after_all_replicas_applied`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___purge_safe_after_all_replicas_applied): Purging is safe once every replica has applied the tombstone.
- [`purge_requires_all_replicas_applied`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___purge_requires_all_replicas_applied): **Red.** Grace is a proxy for that premise, and only that.
- [`a_tombstone_shadows_every_value_at_or_below_its_timestamp`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___a_tombstone_shadows_every_value_at_or_below_its_timestamp): A tombstone shadows every value at or below its timestamp, whoever arrives first.
- [`purging_needs_the_grace_window_and_propagation`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___purging_needs_the_grace_window_and_propagation): Purging needs both conditions.
- [`purging_before_propagation_lets_a_stale_value_win`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___purging_before_propagation_lets_a_stale_value_win): Purging before propagation resurrects.
- [`tombstone_has_four_red_items`](Ferrosa/Storage/Tombstone/#Ferrosa___Storage___Tombstone___tombstone_has_four_red_items): Four obligations are recorded red: a general proof that the merge is order-independent, an `i32` local deletion time that survives every valid TTL, a delete that cannot be resurrected, and a purge path that checks the…

## `Ferrosa.Storage.Streaming`

[The module page](Ferrosa/Storage/Streaming/)

- [`parse_copies_nothing`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___parse_copies_nothing): Parsing copies nothing.
- [`copying_out_of_inbound_does_not_modify_it`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___copying_out_of_inbound_does_not_modify_it): A copy out of inbound data leaves the inbound bytes untouched.
- [`the_pipeline_never_writes_into_inbound_data`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___the_pipeline_never_writes_into_inbound_data): No stage in the pipeline writes into inbound data — neither a socket buffer nor a page read from disk is ever modified in place, so a buffer handed back to the OS is still what arrived.
- [`socket_to_disk_costs_two_copies_and_no_more`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___socket_to_disk_costs_two_copies_and_no_more): The whole path from socket to disk costs exactly two copies: into the buffer, and out of it.
- [`a_mutation_is_transformed_without_being_copied`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_mutation_is_transformed_without_being_copied): And inside the node it costs one: the write itself.
- [`streaming_peak_is_independent_of_the_input`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___streaming_peak_is_independent_of_the_input): Streaming memory does not depend on the input size.
- [`materialising_grows_with_the_input`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___materialising_grows_with_the_input): **Red.** Materialising grows with the input, which is what a row buffer does.
- [`a_row_count_page_bound_is_not_a_memory_bound`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_row_count_page_bound_is_not_a_memory_bound): **Red.** A page bound in rows is not a bound in bytes.
- [`the_page_bound_has_no_byte_ceiling`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___the_page_bound_has_no_byte_ceiling): The same page is a thousand times larger for a thousand-times-wider row.
- [`refilling_a_buffer_invalidates_its_views`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___refilling_a_buffer_invalidates_its_views): Refilling a buffer invalidates its views, so a chunk may be read into a buffer only once every view of the previous contents has been consumed.
- [`refill_is_allowed_only_when_nothing_is_outstanding`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___refill_is_allowed_only_when_nothing_is_outstanding): A buffer is refilled only when no view of its previous contents is outstanding.
- [`a_vector_returning_reader_keeps_every_row_alive`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_vector_returning_reader_keeps_every_row_alive): **Red.** A `Vec` return type has already chosen to materialise.
- [`a_per_chunk_decoder_copies_the_whole_input`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_per_chunk_decoder_copies_the_whole_input): **Red.** A decoder that allocates per chunk copies the entire input.
- [`a_byte_budget_pins_the_memory`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_byte_budget_pins_the_memory): The fix is a byte budget, not a row count: deriving the rows from a budget provably cannot exceed it, for any row width.
- [`a_bounded_queue_never_exceeds_its_capacity`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_bounded_queue_never_exceeds_its_capacity): A bounded queue never exceeds its capacity, so the producer's speed cannot turn into the consumer's memory bill.
- [`without_flow_control_occupancy_follows_the_producer`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___without_flow_control_occupancy_follows_the_producer): **Red.** With no bound on the queue, what a slow client leaves resident is exactly what has been produced and not yet consumed, so occupancy tracks the producer and has no ceiling in it at all.
- [`dropping_when_full_silently_loses_the_item`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___dropping_when_full_silently_loses_the_item): A full queue that drops the push reports the drop to nobody: the consumer receives a result set that looks complete with an item missing from it.
- [`a_queue_drains_in_its_length`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_queue_drains_in_its_length): A queue of `n` items empties in `n` pops, so a consumer that keeps draining always finishes and the backpressure it imposes always clears.
- [`a_full_queue_and_an_idle_consumer_deadlock`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_full_queue_and_an_idle_consumer_deadlock): Backpressure is only safe if the consumer makes progress.
- [`a_window_below_the_product_underdelivers`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_window_below_the_product_underdelivers): A window below the product underdelivers, whatever the disk and the client can do.
- [`a_window_at_the_product_saturates`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_window_at_the_product_saturates): A window at the product saturates the pipe, so the product is the size to aim for.
- [`beyond_the_product_extra_buffers_buy_nothing`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___beyond_the_product_extra_buffers_buy_nothing): Past the product, extra buffers buy nothing — the pipe is already full, so the memory a scan needs is a property of the link, not of the table.
- [`the_window_depends_on_the_link_not_the_table`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___the_window_depends_on_the_link_not_the_table): The window a scan needs is the bandwidth-delay product of the link, with no term for the number of rows: a 64 TiB table and a 64 KiB one ask the coordinator for the same memory.
- [`a_saturated_pipe_leaves_the_slowest_stage_in_charge`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_saturated_pipe_leaves_the_slowest_stage_in_charge): A saturated pipe puts the slowest stage in charge: `SELECT *` then runs as fast as the disk produces and the client pulls, and no faster on either side.
- [`streaming_cannot_beat_the_slowest_stage`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___streaming_cannot_beat_the_slowest_stage): And the delivered rate never exceeds either stage on its own.
- [`one_ack_per_packet_starves_a_high_latency_pipe`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___one_ack_per_packet_starves_a_high_latency_pipe): **Red.** One ack per packet starves a high-latency pipe: 1 500 bytes in flight across a 50 ms RTT delivers 30 KB/s, while 16 coalesced packets deliver 480 KB/s — the same pipe, the same disk.
- [`the_coalescing_depth_is_the_product_over_the_message`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___the_coalescing_depth_is_the_product_over_the_message): So the coalescing depth to aim for is the product over the message size, rounded down.
- [`share_never_exceeds_the_pool`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___share_never_exceeds_the_pool): No client's share exceeds the pool.
- [`the_pool_is_never_overcommitted`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___the_pool_is_never_overcommitted): **Red.** The pool is never overcommitted — the shares sum to the pool exactly, whatever the weights.
- [`a_slow_client_is_throttled_not_starved`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_slow_client_is_throttled_not_starved): A slow client is throttled, not starved: on a 100 MB pool a client at rate 100 and one at rate 10 get 90 MB and 10 MB.
- [`allocation_changes_the_rate_not_the_result`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___allocation_changes_the_rate_not_the_result): The allocator has no path to the result set.
- [`a_short_response_must_say_there_is_more`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___a_short_response_must_say_there_is_more): A short response must say there is more.
- [`streaming_has_twenty_three_items_and_seven_red`](Ferrosa/Storage/Streaming/#Ferrosa___Storage___Streaming___streaming_has_twenty_three_items_and_seven_red): The area records twenty-three obligations of which seven are unmet, and both numbers are pinned here: adding a claim, or closing one, forces the ledger to be updated in the same change.
-/
