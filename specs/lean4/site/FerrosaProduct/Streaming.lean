import Ferrosa

/-!
# Complete results in bounded memory

> *"The 10k result cap is gone. Range scans, SELECT DISTINCT, filtered scans, and COUNT(*) are bounded
> only by your query's own LIMIT … No silent truncation, ever."* — ferrosa.ai

A query result can be larger than any node's memory, so the engine must stream it: memory must not
grow with the size of the input, and a scheduling or memory decision may slow a result down but must
never shorten it.
-/

/-!
## A result is never silently shortened

Allocating less memory to a client changes how fast it receives rows, never which rows it receives.
A response that stops short always says there is more. A slow client is throttled, not starved.
-/

#check @Ferrosa.Storage.Streaming.allocation_changes_the_rate_not_the_result
#check @Ferrosa.Storage.Streaming.a_short_response_must_say_there_is_more
#check @Ferrosa.Storage.Streaming.a_slow_client_is_throttled_not_starved

/-!
## Memory does not grow with the input

Streaming peak memory is independent of the input size; materialising grows with it. Parsing a row
copies none of its bytes, nothing writes into data that arrived from the socket or the disk, and the
socket-to-disk path costs two copies. The shared buffer pool is never overcommitted, and a bounded
queue refuses a push rather than dropping the item.

**Does not hold yet.** The scan's page bound counts rows, not bytes, so it is not a memory bound. A
reader that returns a vector of partitions keeps every row alive, the per-chunk decoder copies its
whole input, and acknowledgements are not coalesced, which starves a high-latency link.
-/

#check @Ferrosa.Storage.Streaming.streaming_peak_is_independent_of_the_input
#check @Ferrosa.Storage.Streaming.materialising_grows_with_the_input
#check @Ferrosa.Storage.Streaming.parse_copies_nothing
#check @Ferrosa.Storage.Streaming.socket_to_disk_costs_two_copies_and_no_more
#check @Ferrosa.Storage.Streaming.the_pool_is_never_overcommitted
#check @Ferrosa.Storage.Streaming.dropping_when_full_silently_loses_the_item
#check @Ferrosa.Storage.Streaming.a_row_count_page_bound_is_not_a_memory_bound
#check @Ferrosa.Storage.Streaming.a_vector_returning_reader_keeps_every_row_alive
#check @Ferrosa.Storage.Streaming.one_ack_per_packet_starves_a_high_latency_pipe

/-!
## Snapshots and versions

A snapshot reads the newest version at or below it, and garbage collection never removes a version a
live snapshot needs. A reader that stops making progress is killed at a timeout and its memory
returned; age alone never kills a reader. Predicate locks are bookkeeping, not blocking locks: no
writer ever waits on a reader, so no reader-writer deadlock can form.

**Does not hold yet.** Compaction can drop the pointer to spilled versions and lose them, and versions
are not yet stored in the record, so a snapshot lookup costs a probe per file.
-/

#check @Ferrosa.Storage.Mvcc.safe_gc_preserves_live_snapshots
#check @Ferrosa.Storage.Mvcc.naive_gc_breaks_a_live_snapshot
#check @Ferrosa.Storage.Mvcc.an_idle_reader_is_killed_at_the_timeout
#check @Ferrosa.Storage.Mvcc.a_progressing_reader_is_never_killed_for_age
#check @Ferrosa.Storage.Mvcc.nothing_waits_so_no_deadlock_can_form
#check @Ferrosa.Storage.Mvcc.spilled_versions_are_lost_if_compaction_drops_the_pointer
#check @Ferrosa.Storage.Mvcc.a_version_per_file_is_not_a_constant_read
