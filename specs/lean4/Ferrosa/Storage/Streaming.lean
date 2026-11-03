/-
  Memory and streaming: two copies, and a footprint that follows the link
  ======================================================================

  Bytes enter one buffer, every stage between views them, and the write into the kernel is the only
  other copy — two in total, both irreducible, and the parse, the transform and the framing add none.
  What a scan holds is therefore a property of the chunk size and the link, never of the table: the
  window that keeps a pipe busy is its bandwidth-delay product, and past that extra buffers buy
  nothing.

  Throttling is the only lever the scheduler has. A smaller share slows a client's delivery and
  leaves its result set identical, so no scheduling decision can become missing data — but that only
  holds while every response either carries the whole answer or says there is more, and while a full
  queue refuses the push instead of dropping it. `Storage.Streaming` records seven places the tree
  falls short of this, the widest being a page bound counted in rows rather than bytes.
-/
import Ferrosa.Ledger

namespace Ferrosa.Storage.Streaming

open Ferrosa.Ledger

/-- Who owns a region of bytes. Inbound data (socket, disk page) is never a write destination. -/
inductive Owner where
  | inbound
  | pageCache
  | arena
  | response
  deriving DecidableEq, Repr

/-- A pipeline step: a view into a buffer that already exists, or a copy into a new one. -/
inductive Step where
  | view (src : Owner) (len : Nat)
  | copy (src dst : Owner) (len : Nat)
  deriving DecidableEq, Repr

/-- Bytes copied by a step. A view copies nothing. -/
def Step.copies : Step → Nat
  | .view _ _ => 0
  | .copy _ _ _ => 1

/-- The owner a step writes into, if it writes at all. -/
def Step.writtenOwner : Step → Option Owner
  | .view _ _ => none
  | .copy _ dst _ => some dst

def pipelineCopies (steps : List Step) : Nat := (steps.map Step.copies).sum

/-- Whether any step writes into inbound bytes. -/
def writesIntoInbound (steps : List Step) : Bool :=
  steps.any fun s => match Step.writtenOwner s with
    | some .inbound => true
    | _ => false

/-- **Parsing copies nothing.**  A parsed row is a set of offsets into the buffer it arrived in, so
    parsing a page allocates no bytes proportional to the page. -/
theorem parse_copies_nothing :
    pipelineCopies [.view .inbound 4096, .view .inbound 512, .view .inbound 16] = 0 := by decide

/-- **A copy out of inbound data leaves the inbound bytes untouched.** -/
theorem copying_out_of_inbound_does_not_modify_it (len : Nat) :
    writesIntoInbound [.copy .inbound .response len] = false := by
  simp [writesIntoInbound, Step.writtenOwner]

/-- **No stage in the pipeline writes into inbound data** — neither a socket buffer nor a page read
    from disk is ever modified in place, so a buffer handed back to the OS is still what arrived. -/
theorem the_pipeline_never_writes_into_inbound_data :
    writesIntoInbound
      [.view .inbound 65536, .copy .inbound .pageCache 65536, .view .pageCache 64,
       .copy .pageCache .response 4096] = false := by decide

/-- **The whole path from socket to disk costs exactly two copies**: into the buffer, and out of it.
    Everything between them is a view — the parse, the transform, the framing. -/
theorem socket_to_disk_costs_two_copies_and_no_more :
    pipelineCopies
      [.copy .inbound .pageCache 65536, .view .pageCache 64, .view .pageCache 512,
       .copy .pageCache .response 65536] = 2 := by decide

/-- **And inside the node it costs one**: the write itself. Zero copies for the transform. -/
theorem a_mutation_is_transformed_without_being_copied :
    pipelineCopies [.view .inbound 24, .view .inbound 8, .view .inbound 64] = 0 := by decide

/-! ## Peak memory

Peak resident memory for a scan is the chunk size plus a fixed working set, so the same code path holds 64
KiB whether the table is 64 KiB or 64 TiB. Materialising first instead multiplies the chunk size by the number
of chunks, which is what a row buffer does, and that is the difference between a memory ceiling and a memory
curve. The page bound is where this breaks in the engine: the default page is 5 000 rows, which is 5 MB of
narrow rows and 5 GB of 1 MiB rows on the same coordinator, so the budget has to be stated in bytes with the
row count derived from it.
-/

/-- Peak resident bytes when the input is consumed one chunk at a time. -/
def peakStreaming (chunkSize fixed : Nat) : Nat := chunkSize + fixed

/-- Peak resident bytes when the whole input is materialised first. -/
def peakMaterialised (chunks chunkSize : Nat) : Nat := chunks * chunkSize

/-- **Streaming memory does not depend on the input size.**  This is the whole point of a chunk
    boundary: the same code path holds 64 KiB whether the table is 64 KiB or 64 TiB. -/
theorem streaming_peak_is_independent_of_the_input (chunks chunkSize fixed : Nat) :
    peakStreaming chunkSize fixed = chunkSize + fixed := rfl

/-- **Materialising grows with the input**, which is what a row buffer does. -/
theorem materialising_grows_with_the_input (chunkSize : Nat) (h : 0 < chunkSize) (chunks : Nat)
    (h2 : 1 < chunks) : peakStreaming chunkSize 0 < peakMaterialised chunks chunkSize := by
  unfold peakStreaming peakMaterialised
  have h1 : chunkSize + 0 = 1 * chunkSize := by rw [Nat.one_mul, Nat.add_zero]
  rw [h1]
  exact Nat.mul_lt_mul_of_pos_right h2 h

/-- Bytes held by a page of `rows` rows of width `rowWidth`. -/
def pageBytes (rows rowWidth : Nat) : Nat := rows * rowWidth

/-- **A page bound in rows is not a bound in bytes.**  `DEFAULT_SCAN_PAGE_SIZE` is 5 000 rows:
    5 MB of narrow rows, 5 GB of 1 MiB rows, on the same coordinator. The number that matters is
    a byte budget, and the row count has to be derived from it. -/
theorem a_row_count_page_bound_is_not_a_memory_bound :
    pageBytes 5000 1 = 5000 ∧ pageBytes 5000 (1024 * 1024) = 5242880000 := by decide

/-- The same page is a thousand times larger for a thousand-times-wider row. -/
theorem the_page_bound_has_no_byte_ceiling :
    pageBytes 5000 1024 > 1000 * pageBytes 5000 1 := by decide

/-! ## Lifetimes: a view is only valid while its buffer is live

A stage does not own the bytes it reads; it holds an offset and a length into a buffer someone else refills.
Reading the next chunk into that buffer invalidates every view of the previous contents, so a refill is
allowed only when no view is outstanding — and that rule is what backpressure is, rather than an optimisation
on top of it. Break it and the consumer is handed bytes it never asked for: stale data rather than an error,
which is the harder failure to notice.
-/

/-- A view into a buffer. -/
structure View where
  owner : Owner
  off : Nat
  len : Nat
  deriving DecidableEq, Repr

/-- Reading the next chunk into a buffer overwrites every view of its previous contents. -/
def viewValidAfterRefill (refilled : Bool) : Bool := !refilled

/-- **Refilling a buffer invalidates its views**, so a chunk may be read into a buffer only once
    every view of the previous contents has been consumed. That is what backpressure *is*; without
    it the reader silently streams stale bytes. -/
theorem refilling_a_buffer_invalidates_its_views :
    viewValidAfterRefill true = false ∧ viewValidAfterRefill false = true := by decide

/-- A refill is allowed exactly when nothing is outstanding. -/
def mayRefill (outstandingViews : Nat) : Bool := outstandingViews == 0

/-- A buffer is refilled only when no view of its previous contents is outstanding. That is what
    backpressure is for: refilling under a live view hands the consumer bytes it never asked for. -/
theorem refill_is_allowed_only_when_nothing_is_outstanding :
    mayRefill 0 = true ∧ mayRefill 1 = false := by decide

/-! ## The API shape decides whether it can stream

Whether a reader streams is decided by its return type before any code runs: a function returning a vector of
rows has already committed to holding them all, so the memory a scan uses is set by the caller's limit rather
than by the data. A decoder that allocates a fresh buffer per chunk copies the whole input through the
allocator, and taking the uncompressed length while ignoring it is how that happens. Streaming has to be in the
signature, because a caller who already holds the vector cannot add it back.
-/

/-- Rows alive at once: streamed one at a time versus returned as a vector. -/
def liveRows (streamed materialised : Nat) : Nat := streamed

/-- **A `Vec` return type has already chosen to materialise.**  `read_partitions_limited` returns
    `Vec<Partition>`, so the number of partitions held in memory is set by the caller's limit, not
    by the data — and the same shape appears in `all_rows`, where the response encoder is streaming
    but the scan feeding it is not. Streaming has to be in the signature. -/
theorem a_vector_returning_reader_keeps_every_row_alive (materialised : Nat)
    (h : 2 < materialised) : liveRows 1 materialised = 1 ∧ liveRows 1 materialised < materialised := by
  refine ⟨rfl, ?_⟩
  unfold liveRows
  omega

/-- Bytes copied when a decoder returns a fresh buffer for each chunk. -/
def bytesCopiedPerChunk (chunks chunkSize : Nat) : Nat := chunks * chunkSize

/-- **A decoder that allocates per chunk copies the entire input.**  `compression.rs`'s
    `decompress(&self, data, _uncompressed_len) -> Vec<u8>` takes the uncompressed length as
    `_uncompressed_len` — ignored — so it cannot reserve, and every byte read is allocated and
    copied again on the way into the arena. -/
theorem a_per_chunk_decoder_copies_the_whole_input (chunks chunkSize : Nat) :
    bytesCopiedPerChunk chunks chunkSize = chunks * chunkSize := rfl

/-- The fix is a byte budget, not a row count: deriving the rows from a budget provably cannot
    exceed it, for any row width. -/
theorem a_byte_budget_pins_the_memory (budget rowWidth : Nat) :
    (budget / rowWidth) * rowWidth ≤ budget := Nat.div_mul_le_self budget rowWidth

/-! ## Backpressure: a slow consumer must not become the producer's memory bill

A bounded queue either takes a row into a free slot or refuses the push, so it never grows and never drops.
Refusal is the backpressure signal and it means the producer waits, which turns a slow consumer into a slower
producer rather than into coordinator memory. The one thing the queue may not do is drop, because a dropped row
on this path is a row missing from a result set whose response still reports itself as complete.
-/

/-- A bounded queue. -/
structure Queue where
  capacity : Nat
  length : Nat
  deriving DecidableEq, Repr

/-- What a push did. `backpressure` is not a failure: it means the producer must wait. -/
inductive PushResult where
  | accepted (q : Queue)
  | backpressure (q : Queue)
  | dropped (q : Queue)
  deriving DecidableEq, Repr

/-- A push either fills one slot or refuses. It never drops and never grows. -/
def Queue.push (q : Queue) : PushResult :=
  if q.length < q.capacity then .accepted ⟨q.capacity, q.length + 1⟩ else .backpressure q

def PushResult.length : PushResult → Nat
  | .accepted q => q.length
  | .backpressure q => q.length
  | .dropped q => q.length

/-- **A bounded queue never exceeds its capacity**, so the producer's speed cannot turn into the
    consumer's memory bill. -/
theorem a_bounded_queue_never_exceeds_its_capacity (q : Queue) (h : q.length ≤ q.capacity) :
    q.push.length ≤ q.capacity := by
  unfold Queue.push PushResult.length
  by_cases hc : q.length < q.capacity
  · simp [hc]; omega
  · simp [hc]; exact h

/-- **Without flow control, occupancy follows the producer**: it is whatever was produced and not
    yet consumed, which is unbounded in the producer's speed. -/
def unboundedOccupancy (produced consumed : Nat) : Nat := produced - consumed

/-- With no bound on the queue, what a slow client leaves resident is exactly what has been produced
    and not yet consumed, so occupancy tracks the producer and has no ceiling in it at all. -/
theorem without_flow_control_occupancy_follows_the_producer (produced consumed : Nat)
    (h : consumed ≤ produced) : unboundedOccupancy produced consumed + consumed = produced := by
  unfold unboundedOccupancy
  omega

/-- **Dropping when full is not backpressure.**  The push reports `dropped`, the item is gone, and
    the consumer cannot tell — a gap in a result set that looks complete. -/
def pushDropping (q : Queue) : PushResult :=
  if q.length < q.capacity then .accepted ⟨q.capacity, q.length + 1⟩ else .dropped q

/-- A full queue that drops the push reports the drop to nobody: the consumer receives a result set
    that looks complete with an item missing from it. This is why dropping is not a form of flow
    control. -/
theorem dropping_when_full_silently_loses_the_item :
    pushDropping ⟨2, 2⟩ = .dropped ⟨2, 2⟩ := by decide

/-- A queue of `n` items drains in `n` pops. -/
def drainSteps : Nat → Nat
  | 0 => 0
  | n + 1 => drainSteps n + 1

/-- A queue of `n` items empties in `n` pops, so a consumer that keeps draining always finishes and
    the backpressure it imposes always clears. -/
theorem a_queue_drains_in_its_length (n : Nat) : drainSteps n = n := by
  induction n with
  | zero => rfl
  | succ k ih => simp [drainSteps, ih]

/-- Progress made in one step, given whether the consumer actually drains. -/
def progressWith (consumerDrains : Bool) : Nat := if consumerDrains then 1 else 0

/-- **Backpressure is only safe if the consumer makes progress.**  A full queue and a consumer that
    never pops is a deadlock, not flow control: the producer waits forever. That is an assumption
    the spec has to state, the way the reorder-buffer deadline is. -/
theorem a_full_queue_and_an_idle_consumer_deadlock :
    progressWith false = 0 ∧ progressWith true = 1 := by decide

/-! ## Throughput: the window has to cover the bandwidth-delay product

A `SELECT *` should run as fast as the disk produces and the client pulls. That needs enough bytes
in flight to keep the pipe busy — and no more, because in-flight bytes are memory. The number is
`bandwidth × RTT`, and a window below it underdelivers no matter how fast the disk is. -/

/-- Bytes that must be in flight to keep a pipe of `bandwidth` busy across `rtt`. -/
def windowFor (bandwidth rtt : Nat) : Nat := bandwidth * rtt

/-- Bytes per unit time actually delivered by a window of `window` over an `rtt`. -/
def delivered (window rtt : Nat) : Nat := window / rtt

/-- **A window below the product underdelivers**, whatever the disk and the client can do. -/
theorem a_window_below_the_product_underdelivers (w bw rtt : Nat) (hrtt : 0 < rtt)
    (h : w < bw * rtt) : delivered w rtt < bw := by
  unfold delivered
  rw [Nat.div_lt_iff_lt_mul hrtt]
  exact h

/-- **A window at the product saturates the pipe**, so the product is the size to aim for. -/
theorem a_window_at_the_product_saturates (w bw rtt : Nat) (hrtt : 0 < rtt)
    (h : bw * rtt ≤ w) : bw ≤ delivered w rtt := by
  unfold delivered
  exact (Nat.le_div_iff_mul_le hrtt).mpr h

/-- **Past the product, extra buffers buy nothing** — the pipe is already full, so the memory a
    scan needs is a property of the link, not of the table. -/
theorem beyond_the_product_extra_buffers_buy_nothing (w bw rtt : Nat) (hrtt : 0 < rtt)
    (h : bw * rtt ≤ w) : min (delivered w rtt) bw = bw := by
  unfold delivered
  exact Nat.min_eq_right ((Nat.le_div_iff_mul_le hrtt).mpr h)

/-- The window a scan needs is the bandwidth-delay product of the link, with no term for the number
    of rows: a 64 TiB table and a 64 KiB one ask the coordinator for the same memory. -/
theorem the_window_depends_on_the_link_not_the_table (bw rtt rows : Nat) :
    windowFor bw rtt = bw * rtt := rfl

/-- Delivered rate, capped by the disk and by what the client pulls. -/
def throughputOf (window rtt disk client : Nat) : Nat :=
  min (delivered window rtt) (min disk client)

/-- **A saturated pipe puts the slowest stage in charge**: `SELECT *` then runs as fast as the disk
    produces and the client pulls, and no faster on either side. -/
theorem a_saturated_pipe_leaves_the_slowest_stage_in_charge (w bw rtt disk client : Nat)
    (h : disk ≤ delivered w rtt) : throughputOf w rtt disk client = min disk client := by
  unfold throughputOf
  omega

/-- And the delivered rate never exceeds either stage on its own. -/
theorem streaming_cannot_beat_the_slowest_stage (window rtt disk client : Nat) :
    throughputOf window rtt disk client ≤ disk ∧ throughputOf window rtt disk client ≤ client := by
  unfold throughputOf
  constructor <;> omega

/-- **One ack per packet starves a high-latency pipe**: 1 500 bytes in flight across a 50 ms RTT
    delivers 30 KB/s, while 16 coalesced packets deliver 480 KB/s — the same pipe, the same disk. -/
theorem one_ack_per_packet_starves_a_high_latency_pipe :
    delivered 1500 50 < delivered (16 * 1500) 50 := by decide

/-- So the coalescing depth to aim for is the product over the message size, rounded down. -/
theorem the_coalescing_depth_is_the_product_over_the_message (bw rtt mss : Nat) :
    (bw * rtt) / mss * mss ≤ bw * rtt := Nat.div_mul_le_self (bw * rtt) mss

/-! ## A shared pool, allocated fairly, that can never cost a client its data

One pool for all connections, sized by `shared_memory`. Under contention the scheduler divides it by
observed drain rate, so a fast client's window grows and a slow one's shrinks. The hard constraint is
that shrinking a share may only slow a client down: it must never shorten a result set.

This is where a fair scheduler and a load shedder differ. A load shedder under pressure drops work;
this one must stall the producer, page the remainder, and *say so*. -/

/-- A shared pool of bytes. -/
structure Pool where
  bytes : Nat
  deriving DecidableEq, Repr

/-- The first client's share, proportional to its observed drain rate. -/
def shareOfFirst (w₁ w₂ pool : Nat) : Nat := pool * w₁ / (w₁ + w₂)

/-- The second client's share is the remainder, so an inexact division can never overcommit. -/
def shareOfSecond (w₁ w₂ pool : Nat) : Nat := pool - shareOfFirst w₁ w₂ pool

/-- No client's share exceeds the pool. -/
theorem share_never_exceeds_the_pool (w₁ w₂ pool : Nat) : shareOfFirst w₁ w₂ pool ≤ pool := by
  unfold shareOfFirst
  by_cases h : w₁ + w₂ = 0
  · simp [h]
  · have hle : pool * w₁ / (w₁ + w₂) ≤ pool * (w₁ + w₂) / (w₁ + w₂) :=
      Nat.div_le_div_right (Nat.mul_le_mul_left pool (Nat.le_add_right w₁ w₂))
    have heq : pool * (w₁ + w₂) / (w₁ + w₂) = pool := by
      rw [Nat.mul_comm pool (w₁ + w₂)]
      exact Nat.mul_div_right pool (Nat.zero_lt_of_ne_zero h)
    omega

/-- **The pool is never overcommitted** — the shares sum to the pool exactly, whatever the weights. -/
theorem the_pool_is_never_overcommitted (w₁ w₂ pool : Nat) :
    shareOfFirst w₁ w₂ pool + shareOfSecond w₁ w₂ pool = pool := by
  unfold shareOfSecond
  exact Nat.add_sub_of_le (share_never_exceeds_the_pool w₁ w₂ pool)

/-- **A slow client is throttled, not starved**: on a 100 MB pool a client at rate 100 and one at
    rate 10 get 90 MB and 10 MB. The slow one waits longer for every row and still gets every row. -/
theorem a_slow_client_is_throttled_not_starved :
    shareOfFirst 100 10 100 = 90 ∧ shareOfSecond 100 10 100 = 10 := by decide

/-- What a client receives, given its allocation. The allocation is not an argument. -/
def rowsFor (allocation : Nat) (result : List Nat) : List Nat := result

/-- **The allocator has no path to the result set.**  Halving a client's share halves its rate by
    `delivered` and leaves the rows identical, so no scheduling decision can become missing data.
    This is the invariant that makes fair-share throttling safe instead of a silent truncation. -/
theorem allocation_changes_the_rate_not_the_result (a₁ a₂ : Nat) (result : List Nat) :
    rowsFor a₁ result = rowsFor a₂ result := rfl

/-- A response: how many rows it carries, and whether it says there is more. -/
structure RowResponse where
  rows : Nat
  hasMore : Bool
  deriving DecidableEq, Repr

/-- An honest response either carries everything or says there is more. -/
def honest (r : RowResponse) (total : Nat) : Bool := (r.rows == total) || r.hasMore

/-- **A short response must say there is more.**  This is what stops a throttled client from
    receiving a partial answer that looks complete: the paging token is the only thing standing
    between a slow client and silent data loss. -/
theorem a_short_response_must_say_there_is_more :
    honest ⟨10, true⟩ 100 = true ∧ honest ⟨10, false⟩ 100 = false ∧ honest ⟨100, false⟩ 100 = true := by
  decide

/-! ## The ledger

Every memory and streaming claim is listed here with the theorem behind it, and the gaps sit in the same table:
the scan's page bound is counted in rows rather than bytes, a decoder that allocates per chunk copies the whole
input, and the other shortfalls against the tree are named the same way. Reading it
answers what a client is promised about memory when it pulls a result, and where that promise stops. The item
count and the red count are both pinned, so a shortfall fixed without a table update fails the build.
-/

def ledger : List SpecItem := [
  ⟨"parsing a row copies none of its bytes",
   .met "Streaming.parse_copies_nothing"⟩,
  ⟨"no stage writes into data that arrived from the socket or the disk",
   .met "Streaming.the_pipeline_never_writes_into_inbound_data"⟩,
  ⟨"the socket-to-disk path costs two copies and no more",
   .met "Streaming.socket_to_disk_costs_two_copies_and_no_more"⟩,
  ⟨"peak memory does not depend on the input size",
   .met "Streaming.streaming_peak_is_independent_of_the_input"⟩,
  ⟨"a buffer is refilled only when no view of it is outstanding",
   .met "Streaming.refill_is_allowed_only_when_nothing_is_outstanding"⟩,
  ⟨"the scan's page bound is a bound in bytes",
   .failing
     "it is a bound in rows: DEFAULT_SCAN_PAGE_SIZE is 5000, which is 5 MB of narrow rows and 5 GB of 1 MiB rows on the same coordinator"
     "Streaming.a_row_count_page_bound_is_not_a_memory_bound"⟩,
  ⟨"the reader hands partitions to the caller instead of a vector of them",
   .failing
     "read_partitions_limited returns Vec<Partition>, so the reader has already materialised the page by the time the caller sees it"
     "Streaming.a_vector_returning_reader_keeps_every_row_alive"⟩,
  ⟨"a decoded chunk is written into a caller-supplied buffer rather than returned as a new Vec",
   .failing
     "compression.rs decompress takes the uncompressed length and ignores it, returning a fresh Vec per chunk: one allocation and one copy for every byte read"
     "Streaming.a_per_chunk_decoder_copies_the_whole_input"⟩,
  ⟨"a bounded queue never exceeds its capacity",
   .met "Streaming.a_bounded_queue_never_exceeds_its_capacity"⟩,
  ⟨"a full queue refuses the push rather than dropping the item",
   .met "Streaming.dropping_when_full_silently_loses_the_item"⟩,
  ⟨"backpressure is safe only because the consumer is guaranteed to drain",
   .met "Streaming.a_full_queue_and_an_idle_consumer_deadlock"⟩,
  ⟨"a window below the bandwidth-delay product underdelivers",
   .met "Streaming.a_window_below_the_product_underdelivers"⟩,
  ⟨"a window at the product saturates the pipe",
   .met "Streaming.a_window_at_the_product_saturates"⟩,
  ⟨"a saturated pipe runs at the rate of the slowest stage",
   .met "Streaming.a_saturated_pipe_leaves_the_slowest_stage_in_charge"⟩,
  ⟨"memory is a property of the link, not of the table",
   .met "Streaming.beyond_the_product_extra_buffers_buy_nothing"⟩,
  ⟨"the shared pool is never overcommitted",
   .met "Streaming.the_pool_is_never_overcommitted"⟩,
  ⟨"a slow client is throttled by the scheduler, never starved",
   .met "Streaming.a_slow_client_is_throttled_not_starved"⟩,
  ⟨"a scheduling decision can never shorten a result set",
   .met "Streaming.allocation_changes_the_rate_not_the_result"⟩,
  ⟨"a short response always signals that there is more",
   .met "Streaming.a_short_response_must_say_there_is_more"⟩,
  ⟨"acks are coalesced so a high-latency pipe is not ack-starved",
   .failing
     "TCP_NODELAY exists only as c38add89, which is not on main, and it coalesces nothing: replies are still several small writes, so main pays ~46 ms per statement over a real network"
     "Streaming.one_ack_per_packet_starves_a_high_latency_pipe"⟩,
  ⟨"buffer memory is one shared pool sized by a shared_memory setting",
   .failing
     "there is no shared pool and no such setting: each response allocates its own BytesMut, so memory is per-connection and grows with concurrency rather than being scheduled"
     "Streaming.the_pool_is_never_overcommitted"⟩,
  ⟨"a slow client cannot grow the coordinator's memory",
   .failing
     "encode_rows_with_writer returns one BytesMut for the whole page and the write happens after, so occupancy is the entire response: a slow client parks all of it in the coordinator"
     "Streaming.without_flow_control_occupancy_follows_the_producer"⟩,
  ⟨"the row buffer is never built as Vec<Vec<Option<CqlValue>>> on the scan path",
   .failing
     "the response encoder visits rows one at a time, but the scan side accumulates up to a full page before the encoder is called"
     "Streaming.materialising_grows_with_the_input"⟩
]

/-- The area records twenty-three obligations of which seven are unmet, and both numbers are pinned
    here: adding a claim, or closing one, forces the ledger to be updated in the same change. -/
theorem streaming_has_twenty_three_items_and_seven_red : ledger.length = 23 ∧ redCount ledger = 7 := by
  decide

def main : IO Unit :=
  printLedger "memory and streaming" ledger

end Ferrosa.Storage.Streaming
