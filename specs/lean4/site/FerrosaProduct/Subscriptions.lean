import Ferrosa

/-!
# Subscriptions and change data capture

> *"SUBSCRIBE social.users EVERY 5s; SUBSCRIBE social.users DELTA; … Treat arbitrary-query CQL/Cypher
> streaming as proposed or partially covered until the verification plans land."* — ferrosa.ai

A subscriber is a consumer that can be slower than the writer. The guarantees a user needs are that a
slow subscriber never slows the database down, never runs it out of memory, and is always told what it
missed instead of silently skipping it.
-/

/-!
## The three shapes of SUBSCRIBE

`SUBSCRIBE` without an `ON` clause polls a snapshot, no more often than every 500 ms, and can miss an
intermediate state that a change stream would deliver. `ON LOCAL` and `ON COMMITTED` select the two
change streams, and only those can report a gap.
-/

#check @Ferrosa.Cql.Subscribe.the_default_subscribe_mode_is_a_polled_snapshot
#check @Ferrosa.Cql.Subscribe.snapshot_polling_cannot_be_tighter_than_half_a_second
#check @Ferrosa.Cql.Subscribe.a_snapshot_misses_what_a_stream_delivers
#check @Ferrosa.Cql.Subscribe.on_local_and_on_committed_select_different_streams

/-!
## What a change stream promises

`ON COMMITTED` never announces a transaction that did not commit. Delivery is at least once, and the
consumer deduplicates on the mutation id. A subscriber that falls behind is told how many events it
missed and where to resume from; resuming delivers exactly the outstanding events. A slow subscriber
cannot stall the writer.
-/

#check @Ferrosa.Cdc.an_uncommitted_transaction_is_never_announced
#check @Ferrosa.Cdc.at_least_once_delivers_every_committed_id_at_least_once
#check @Ferrosa.Cdc.a_gap_is_named_and_never_silent
#check @Ferrosa.Commons.Stream.resuming_from_a_resumable_gap_loses_nothing
#check @Ferrosa.Cdc.a_slow_subscriber_cannot_stall_the_writer

/-!
## A slow consumer cannot take the node down

Consumers buffer from one shared memory pool and spill to disk past their fair share; for any backlog,
resident memory stays inside the pool, and every event is either resident or spilled. A consumer that
re-attaches is replayed the spill and then the buffer, in order, from its last acknowledged position.
The share is adjusted by a proportional controller whose gain is proved stable.

**Does not hold yet.** The implementation does not yet spill a slow consumer's overflow to disk, the
wire gap signal does not carry the resume position, and a cancelled subscription does not report its
final cursor.
-/

#check @Ferrosa.Commons.Flow.a_slow_consumer_cannot_oom_the_node
#check @Ferrosa.Commons.Flow.a_smaller_allocation_does_not_lose_events
#check @Ferrosa.Commons.Flow.a_reattach_resumes_from_the_last_ack
#check @Ferrosa.Commons.Flow.a_unit_gain_settles_in_one_step
#check @Ferrosa.Commons.Flow.a_gain_above_one_oscillates

/-!
## Arrow Flight

A Flight stream ticket is not a token-range read. A gap is never sent as an empty record batch, a
resumable gap becomes the next ticket so CQL and Flight agree on the cursor, and a slow Flight
consumer cannot throttle the write path.

**Does not hold yet.** The Flight planner cannot produce a stream ticket.
-/

#check @Ferrosa.Flight.a_gap_is_never_an_empty_batch
#check @Ferrosa.Flight.a_resumable_gap_becomes_the_next_ticket
#check @Ferrosa.Flight.flight_backpressure_cannot_reach_the_writer
