/-!
# Register III: What leaves the cluster

What an operator subscribes to once a write lands: change-data-capture and flight-style subscribers, the streaming reader they page through, and the three feedback loops that decide how much goes out per tick. The loops are specified as recurrences, so statements about overshoot and steady-state offset are arithmetic on a closed form, not simulation results.

Tick a theorem once you have reviewed its statement and docstring on its module page. Ticks are kept in this browser only.


## `Ferrosa.Cdc.Subscribe`

[The module page](Ferrosa/Cdc/Subscribe/)

- [`a_gap_is_named_and_never_silent`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___a_gap_is_named_and_never_silent): A gap is named, never silent.
- [`an_uncommitted_transaction_is_never_announced`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___an_uncommitted_transaction_is_never_announced): A transaction that has not committed is never announced.
- [`a_slow_subscriber_cannot_stall_the_writer`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___a_slow_subscriber_cannot_stall_the_writer): A slow subscriber cannot slow the writer.
- [`at_least_once_delivers_every_committed_id_at_least_once`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___at_least_once_delivers_every_committed_id_at_least_once): The same mutation delivered twice collapses to one entry, so a consumer that dedups on `mutationId` sees each committed change exactly once however many times the bus hands it over.
- [`the_two_streams_order_differently`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___the_two_streams_order_differently): The same two mutations arrive in opposite orders on `WrittenOnNode` and `CommittedToCluster`, so no ordering of one stream can be read as the ordering of the other: e2 is written later and commits first.
- [`merging_the_streams_needs_a_consumer_side_order`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___merging_the_streams_needs_a_consumer_side_order): A consumer that merges the two streams finds them in disagreement, so the ordering key has to be chosen on the client side — the server holds no position from which one cursor over both would exist.
- [`an_over_limit_consumer_was_not_stalled`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___an_over_limit_consumer_was_not_stalled): A consumer can sit over its file limit while still making progress — evicted for capacity, not killed for idleness.
- [`a_stalled_consumer_is_under_its_file_limit`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___a_stalled_consumer_is_under_its_file_limit): A stalled consumer can be far under its file limit — killed for idleness, not for capacity.
- [`the_two_limits_are_independent`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___the_two_limits_are_independent): The limits are independent: neither implies the other, so they cannot share a setting.
- [`cdc_has_four_red_items`](Ferrosa/Cdc/Subscribe/#Ferrosa___Cdc___cdc_has_four_red_items): The area's red count is pinned at four: no controller behind the fair share, a bus that drops instead of spilling, a gap carrying no position to replay from, and a cancellation carrying no final cursor.

## `Ferrosa.Flight.Subscribe`

[The module page](Ferrosa/Flight/Subscribe/)

- [`a_stream_ticket_is_not_a_range_select`](Ferrosa/Flight/Subscribe/#Ferrosa___Flight___a_stream_ticket_is_not_a_range_select): **Red.** A stream ticket is not a range ticket.
- [`a_gap_is_never_an_empty_batch`](Ferrosa/Flight/Subscribe/#Ferrosa___Flight___a_gap_is_never_an_empty_batch): A gap is never encoded as an empty batch.
- [`flight_backpressure_cannot_reach_the_writer`](Ferrosa/Flight/Subscribe/#Ferrosa___Flight___flight_backpressure_cannot_reach_the_writer): A client's drain rate is not an argument to the producer's progress, so a slow `do_get` consumer cannot throttle the write path the way a slow scan throttles only itself.
- [`a_resumable_gap_becomes_the_next_ticket`](Ferrosa/Flight/Subscribe/#Ferrosa___Flight___a_resumable_gap_becomes_the_next_ticket): A resumable gap becomes the next stream ticket carrying the same cursor, and a reset becomes no ticket at all, so the position `Cdc.Subscribe` reports is the position Flight's next `do_get` asks from.
- [`flight_has_one_red_item`](Ferrosa/Flight/Subscribe/#Ferrosa___Flight___flight_has_one_red_item): The area records one unmet obligation — there is no stream ticket type and no subscription `do_get` — and the count is pinned so that implementing it without updating the ledger fails the build.

## `Ferrosa.Commons.Stream`

[The module page](Ferrosa/Commons/Stream/)

- [`a_gap_is_either_resumable_or_an_explicit_reset`](Ferrosa/Commons/Stream/#Ferrosa___Commons___Stream___a_gap_is_either_resumable_or_an_explicit_reset): **Red.** A gap always says which kind it is.
- [`resuming_from_a_resumable_gap_loses_nothing`](Ferrosa/Commons/Stream/#Ferrosa___Commons___Stream___resuming_from_a_resumable_gap_loses_nothing): Resuming from a resumable gap loses nothing: the outstanding events are exactly the ones the gap counted, delivered in order from the position it named.
- [`a_cancellation_reports_the_position_to_resume_from`](Ferrosa/Commons/Stream/#Ferrosa___Commons___Stream___a_cancellation_reports_the_position_to_resume_from): **Red.** A cancellation is not a loss.

## `Ferrosa.Commons.Hll`

[The module page](Ferrosa/Commons/Hll/)

- [`merge_is_commutative`](Ferrosa/Commons/Hll/#Ferrosa___Commons___Hll___merge_is_commutative): **Red.** Two partial sketches produce the same registers whichever order they are folded in, so per-partition results can be combined in arrival order.
- [`merge_is_associative`](Ferrosa/Commons/Hll/#Ferrosa___Commons___Hll___merge_is_associative): Grouping does not matter either, so a fan-out of partials can be folded pairwise in any tree shape and still give one sketch.
- [`merge_is_idempotent`](Ferrosa/Commons/Hll/#Ferrosa___Commons___Hll___merge_is_idempotent): A retried merge cannot inflate the estimate: merging the same partial twice merges it once.
- [`the_footprint_does_not_grow_with_the_distinct_count`](Ferrosa/Commons/Hll/#Ferrosa___Commons___Hll___the_footprint_does_not_grow_with_the_distinct_count): **Red.** The footprint has no term in the number of distinct values, which is what makes the sketch survive a million-value group.
- [`hll_has_two_red_items`](Ferrosa/Commons/Hll/#Ferrosa___Commons___Hll___hll_has_two_red_items): Two obligations are unmet here: the `1.04/√m` error bound, which needs a probability model this corpus does not build, and the sketch itself, which the tree does not have — `count.rs` probes a Bloom filter and returns…

## `Ferrosa.Commons.Flow`

[The module page](Ferrosa/Commons/Flow/)

- [`a_slower_consumer_is_given_less_memory`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_slower_consumer_is_given_less_memory): A slower consumer is given less memory, which is the fairness rule rather than a punishment: a consumer that drains slowly needs less resident space to stay saturated.
- [`the_shares_do_not_overcommit_the_pool`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_shares_do_not_overcommit_the_pool): The shares never overcommit the pool.
- [`a_consumer_within_its_allocation_never_touches_disk`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_consumer_within_its_allocation_never_touches_disk): A consumer inside its fair share never touches disk.
- [`spilling_begins_exactly_at_the_allocation`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___spilling_begins_exactly_at_the_allocation): Spilling begins exactly at the fair share, one event past it, not before.
- [`a_consumer_over_its_share_costs_disk_not_the_pool`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_consumer_over_its_share_costs_disk_not_the_pool): **Red.** A consumer over its share is bounded by its share, not by its backlog — so however far behind it falls, it cannot grow the pool.
- [`a_slow_consumer_cannot_oom_the_node`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_slow_consumer_cannot_oom_the_node): A slow consumer cannot OOM the node.
- [`a_smaller_allocation_does_not_lose_events`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_smaller_allocation_does_not_lose_events): A smaller allocation does not lose events: the same 10,000 events are all resident-or-spilled at either share, so allocation changes the rate and not the completeness.
- [`resuming_replays_the_spill_before_the_buffer`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___resuming_replays_the_spill_before_the_buffer): Resuming replays the spill before the buffer, so a re-attaching consumer sees its events in the order they were produced, whichever medium each one is on.
- [`an_ack_releases_the_oldest_first`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___an_ack_releases_the_oldest_first): An acknowledgement releases the oldest events first, so the buffer only ever holds the newest events the consumer has not yet seen.
- [`a_reattach_resumes_from_the_last_ack`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_reattach_resumes_from_the_last_ack): A re-attach resumes from the last acknowledgement, spill then buffer, with nothing missing in between.
- [`a_dead_consumer_cannot_consume_the_disk`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_dead_consumer_cannot_consume_the_disk): A dead consumer cannot consume the disk, whatever its backlog: the spill file is capped, and the cap is the one remaining loss path — a counted, signalled eviction.
- [`a_consumer_is_bounded_by_ram_then_file`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_consumer_is_bounded_by_ram_then_file): A consumer's total footprint is bounded by its pool share plus one file, so neither a slow nor a dead consumer can grow without limit: RAM by the share, disk by `max_subscriber_file`.
- [`the_default_evaluation_period_is_five_seconds`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_default_evaluation_period_is_five_seconds): The fair share is recalculated every five seconds by default, and that one number sets both the decision rate and, through the sampling period, how stale an observation can be.
- [`a_pool_above_ninety_percent_evaluates_early`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_pool_above_ninety_percent_evaluates_early): A pool above 90% is re-evaluated without waiting for the period, which is what keeps consumption below 90% rather than discovering it at 100%.
- [`a_quiet_pool_waits_for_the_period`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_quiet_pool_waits_for_the_period): Exactly 90% is not yet over the watermark; a quiet pool waits for the period to elapse.
- [`the_move_toward_the_target_is_gradual`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_move_toward_the_target_is_gradual): The move toward a new share is gradual, and never overshoots it in either direction.
- [`shrinking_a_share_loses_nothing`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___shrinking_a_share_loses_nothing): A recalculation can shrink a share, and shrinking loses nothing: the resident excess moves to disk, so a smaller allocation costs latency and never data.
- [`at_full_allocation_delivery_is_throttled`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___at_full_allocation_delivery_is_throttled): At 100% allocation delivery is throttled and the overflow spills; one byte short of it, delivery flows and the consumer reads at its own pace.
- [`throttling_the_consumer_leaves_the_writer_alone`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___throttling_the_consumer_leaves_the_writer_alone): Throttling the consumer never throttles the producer: the write path's progress does not consult pool occupancy at all, so the ceiling cannot turn into write unavailability.
- [`a_tightening_pool_steps_by_bytes`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_tightening_pool_steps_by_bytes): Above 80% the step is byte-limited, so a tightening pool is never yanked toward a new share in one tick.
- [`at_ninety_percent_the_step_is_half_the_gap`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___at_ninety_percent_the_step_is_half_the_gap): At 90% the step is half the remaining gap.
- [`a_step_never_exceeds_the_gap`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_step_never_exceeds_the_gap): The step never exceeds the distance it has to travel, so the band rules cannot overshoot a target even when the gap is smaller than the step.
- [`a_drain_rate_alone_cannot_decide_safety`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_drain_rate_alone_cannot_decide_safety): The drain rate alone cannot say whether a consumer will fill the buffer.
- [`a_consumer_at_half_the_production_rate_still_fills_the_buffer`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_consumer_at_half_the_production_rate_still_fills_the_buffer): A consumer at half the production rate still exhausts the buffer — just more slowly — which is the case a drain-rate-only scheduler misses.
- [`only_the_gap_between_production_and_drain_decides`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___only_the_gap_between_production_and_drain_decides): What decides is the gap, not either rate: doubling both rates leaves the time to fill unchanged.
- [`the_sampling_period_is_half_the_evaluation_period`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_sampling_period_is_half_the_evaluation_period): Occupancy is sampled every 2.5 s, derived from the evaluation period rather than configured beside it, so an operator who changes the period changes the sampling rate with it and the ratio cannot drift.
- [`two_samples_tile_the_evaluation_window`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___two_samples_tile_the_evaluation_window): Two samples tile the evaluation window exactly, so every decision has a fresh observation covering the period it decides over.
- [`a_rate_needs_two_samples_per_evaluation_window`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_rate_needs_two_samples_per_evaluation_window): A rate needs two samples: one sample is an occupancy reading with no trend, so the sampling rate cannot be equal to the decision rate and still produce a rate.
- [`an_observation_is_never_older_than_half_a_decision_period`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___an_observation_is_never_older_than_half_a_decision_period): A stall is observed within half a decision period, which is the whole reason for sampling at 2x: the decision may be old, the observation it acts on is not.
- [`a_unit_gain_settles_in_one_step`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_unit_gain_settles_in_one_step): A unit-gain proportional controller settles in one step, without overshoot.
- [`a_gain_above_one_oscillates`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_gain_above_one_oscillates): A gain above one overshoots, and a gain of two alternates forever — the stability boundary the byte cap exists to enforce.
- [`a_proportional_loop_holds_a_steady_offset`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_proportional_loop_holds_a_steady_offset): **Red.** Against a steady inflow the loop settles with the error still standing at `inflow / kp` — 10 at a gain of 2, 5 at a gain of 4 — and that offset is the operating point the watermark wants rather than a miss to be drive…
- [`the_periodic_step_already_integrates`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_periodic_step_already_integrates): Repeating "move half the remaining gap" every period shrinks the gap geometrically — 1024, 512, 64 after four periods, 1 after ten — so the recurring step is the integrating action and an explicit integral term would…
- [`an_integral_winds_up_under_saturation`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___an_integral_winds_up_under_saturation): An integral term, if enabled, winds up while the actuator saturates: three times the error accumulates, the actuator only ever spends the cap, and the surplus is still owed when the plant frees up.
- [`anti_windup_stops_the_accumulation`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___anti_windup_stops_the_accumulation): Anti-windup is mandatory if `ki` is non-zero, not tuning: without it the surplus is spent on release and lands as a spill burst.
- [`a_derivative_would_carry_the_noise`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___a_derivative_would_carry_the_noise): And that is why it is excluded: a ten-unit occupancy jitter *is* a ten-unit demand, so at 2.5s sampling the derivative cannot distinguish noise from a trend.
- [`the_controller_needs_a_monotone_plant`](Ferrosa/Commons/Flow/#Ferrosa___Commons___Flow___the_controller_needs_a_monotone_plant): The loop needs a monotone plant.

## `Ferrosa.Commons.FrontEnd`

[The module page](Ferrosa/Commons/FrontEnd/)

- [`every_commons_target_is_shared_by_at_least_two_front_ends`](Ferrosa/Commons/FrontEnd/#Ferrosa___Commons___Refactors___every_commons_target_is_shared_by_at_least_two_front_ends): Every target in the list is shared by at least two front ends, which is the test for belonging in commons.
- [`there_are_twelve_commons_targets`](Ferrosa/Commons/FrontEnd/#Ferrosa___Commons___Refactors___there_are_twelve_commons_targets): The inventory is twelve entries; adding a thirteenth without a shared justification fails here.
-/
