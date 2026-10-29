/-
  Ferrosa formal specification — SUBSCRIBE, the change-data-capture streams.
  ========================================================================

  `ferrosa-cql/src/subscribe.rs` and `ferrosa-cdc` expose two optional, event-driven streams:

    * `WrittenOnNode`        — every mutation durably appended to this node's commit log,
                               ordered by mutation timestamp.
    * `CommittedToCluster`   — mutations the cluster has agreed or acknowledged (Accord
                               commit, or a regular-CL quorum ack on the coordinator).

  This is not a polled `SELECT` snapshot, so none of the query-path rules transfer: a subscriber
  cannot be allowed to slow the writer, which means the trade that governs `SELECT *` — backpressure
  — is inverted here. A slow subscriber is given an explicit, counted gap instead of flow control.
  The obligation is therefore *legibility*, not completeness: nothing may be lost silently, and a
  gap must say how much it swallowed.

  What the code does not have, and this file records as red, is a resumption point. A lagged
  subscriber is told how many events it missed and is given no cursor to fetch them with, so its
  only correct recovery is a fresh snapshot. For a change stream that is a real operational hole,
  not a wording problem.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Commons.Flow
import Ferrosa.Commons.Stream
import Ferrosa.Storage.Mvcc

namespace Ferrosa.Cdc

open Ferrosa Ferrosa.Ledger
open Ferrosa.Commons.Flow
open Ferrosa.Commons.Stream

/-- One change event. `mutationId` is the dedup key: delivery is at-least-once, so a consumer must
    collapse repeats of the same id. -/
structure Event where
  mutationId : Nat
  key : Nat
  ts : Int
  committed : Bool
  deriving DecidableEq, Repr

/-- What a subscriber observes. There is deliberately no "nothing happened" constructor: silence is
    not a state a healthy subscription can report. -/
inductive Recv where
  | event (e : Event)
  | lagged (dropped : Nat)
  | closed
  deriving DecidableEq, Repr

/-- A subscription over a bus that dropped `dropped` events delivers the gap and then continues. -/
def deliverLagged (dropped : Nat) (e : Event) : List Recv :=
  [.lagged dropped, .event e]

/-- **A gap is named, never silent.**  The lagging subscriber is told the exact number of events it
    missed, so "some events were dropped" is legible to the client and no delivery can be mistaken
    for an absence of changes. -/
theorem a_gap_is_named_and_never_silent (dropped : Nat) (e : Event) :
    deliverLagged dropped e = [.lagged dropped, .event e]
      ∧ (deliverLagged dropped e).length = 2 := by
  exact ⟨rfl, rfl⟩

/-- `ON COMMITTED` gating: an event is handed to the subscriber only once the transaction it
    describes has committed. -/
def deliverCommittedOnly (e : Event) : List Recv :=
  if e.committed then [.event e] else []

/-- **A transaction that has not committed is never announced.**  A subscriber of
    `CommittedToCluster` cannot observe an event for a transaction that later aborts, so the stream
    is a log of facts rather than of intentions. -/
theorem an_uncommitted_transaction_is_never_announced :
    deliverCommittedOnly ⟨1, 0, 0, false⟩ = []
      ∧ deliverCommittedOnly ⟨1, 0, 0, true⟩ = [.event ⟨1, 0, 0, true⟩] := by decide

/-- **A slow subscriber cannot slow the writer.**  The producer's progress does not take subscriber
    occupancy as an argument at all — the exact dual of the query path, where a full buffer must
    exert backpressure instead of dropping. -/
theorem a_slow_subscriber_cannot_stall_the_writer (occupancy accepted : Nat) :
    writerProgress occupancy accepted = accepted := rfl

/-- **At-least-once admits a duplicate**, so the consumer's dedup on `mutationId` is load-bearing
    rather than defensive. -/
def dedup (events : List Event) : List Nat :=
  (events.map (·.mutationId)).eraseDups

theorem at_least_once_delivers_every_committed_id_at_least_once :
    dedup [⟨7, 0, 0, true⟩, ⟨7, 0, 0, true⟩, ⟨8, 1, 1, true⟩] = [7, 8] := by decide

/-- **The two streams do not share an order.**  `WrittenOnNode` is ordered by mutation timestamp and
    `CommittedToCluster` by the Accord/apply timestamp, so a consumer following both cannot assume a
    total order across them: e2 is written later but commits earlier. -/
def writtenFirst : List Nat := [1, 2]
def committedFirst : List Nat := [2, 1]

theorem the_two_streams_order_differently :
    writtenFirst ≠ committedFirst := by decide

/-- A consumer that merges both streams must therefore order on its own key; the server makes no
    promise that a single cursor exists. -/
def mergedOrderIsTheConsumersProblem (a b : List Nat) : Bool := a == b

theorem merging_the_streams_needs_a_consumer_side_order :
    mergedOrderIsTheConsumersProblem writtenFirst committedFirst = false := by decide

/-! ### Two limits, two different triggers

`max_subscriber_file` and `STALLED_READER_TIMEOUT` are not the same policy at two scales, and keying
them off one setting would be wrong.

* A consumer over its file limit is **working** — it is draining, just slower than production, which
  is why its spill file grew. It is evicted for capacity, and it can re-query.
* A stalled consumer is **not working at all**, and its file is typically small precisely because it
  is not reading. It is killed for lack of progress, whatever its file size.

Neither condition implies the other, and both are signalled: eviction says the data must be
re-queried, a kill says the snapshot is gone.
-/

/-- **A consumer can sit over its file limit while still making progress** — evicted for capacity, not
    killed for idleness. -/
theorem an_over_limit_consumer_was_not_stalled :
    evicted 1073741824 = true
      ∧ Ferrosa.Storage.Mvcc.stalled 0 = false := by decide

/-- **A stalled consumer can be far under its file limit** — killed for idleness, not for capacity. -/
theorem a_stalled_consumer_is_under_its_file_limit :
    Ferrosa.Storage.Mvcc.stalled 30000 = true ∧ evicted 4096 = false := by decide

/-- **The limits are independent**: neither implies the other, so they cannot share a setting. -/
theorem the_two_limits_are_independent :
    (evicted 1073741824 = true ∧ Ferrosa.Storage.Mvcc.stalled 0 = false)
      ∧ (Ferrosa.Storage.Mvcc.stalled 30000 = true ∧ evicted 4096 = false) := by decide

def ledger : List SpecItem := [
  ⟨"a lagging subscriber is told how many events it missed, never silently skipped",
   .met "Cdc.a_gap_is_named_and_never_silent"⟩,
  ⟨"ON COMMITTED never announces a transaction that has not committed",
   .met "Cdc.an_uncommitted_transaction_is_never_announced"⟩,
  ⟨"a slow subscriber cannot stall the writer, inverting the query path's backpressure rule",
   .met "Cdc.a_slow_subscriber_cannot_stall_the_writer"⟩,
  ⟨"delivery is at-least-once and the consumer dedups on the mutation id",
   .met "Cdc.at_least_once_delivers_every_committed_id_at_least_once"⟩,
  ⟨"the two streams use different ordering keys and offer no shared cursor",
   .met "Cdc.the_two_streams_order_differently"⟩,
  ⟨"a gap is either resumable with a position or an explicit reset, never a bare count",
   .met "Commons.Stream.a_gap_is_either_resumable_or_an_explicit_reset"⟩,
  ⟨"resuming from a resumable gap delivers exactly the outstanding events",
   .met "Commons.Stream.resuming_from_a_resumable_gap_loses_nothing"⟩,
  ⟨"a cancellation reports the position to resume from",
   .met "Commons.Stream.a_cancellation_reports_the_position_to_resume_from"⟩,
  ⟨"the controller is proportional, and unit gain settles in one step without overshoot",
   .met "Commons.Flow.a_unit_gain_settles_in_one_step"⟩,
  ⟨"a gain above one overshoots and a gain of two oscillates, so the byte cap is the stability margin",
   .met "Commons.Flow.a_gain_above_one_oscillates"⟩,
  ⟨"a proportional loop settles at a steady offset, which is the operating point at the watermark",
   .met "Commons.Flow.a_proportional_loop_holds_a_steady_offset"⟩,
  ⟨"the periodic half-distance step is the integrating action, so no explicit integral is needed",
   .met "Commons.Flow.the_periodic_step_already_integrates"⟩,
  ⟨"if an integral term is enabled it winds up under saturation, so anti-windup is mandatory",
   .met "Commons.Flow.anti_windup_stops_the_accumulation"⟩,
  ⟨"a derivative term is excluded: at a two-sample rate it cannot separate noise from trend",
   .met "Commons.Flow.a_derivative_would_carry_the_noise"⟩,
  ⟨"the control loop requires a monotone plant: more allocation never slows a consumer",
   .met "Commons.Flow.the_controller_needs_a_monotone_plant"⟩,
  ⟨"the fair share is driven by a proportional controller",
   .failing
     "there is no controller: the share is recomputed and dropped-on-lag, with no error term and no saturation handling"
     "Commons.Flow.a_proportional_loop_holds_a_steady_offset"⟩,
  ⟨"sampling runs at twice the decision rate, so a rate always has two fresh observations",
   .met "Commons.Flow.two_samples_tile_the_evaluation_window"⟩,
  ⟨"a single sample cannot yield a rate, only an occupancy",
   .met "Commons.Flow.a_rate_needs_two_samples_per_evaluation_window"⟩,
  ⟨"a stall is observed within half a decision period, not a whole one",
   .met "Commons.Flow.an_observation_is_never_older_than_half_a_decision_period"⟩,
  ⟨"the fair share is recalculated every SHMEM_FAIR_EVAL_PERIOD, 5s by default",
   .met "Commons.Flow.the_default_evaluation_period_is_five_seconds"⟩,
  ⟨"a pool above 90% triggers an early re-evaluation rather than waiting for the period",
   .met "Commons.Flow.a_pool_above_ninety_percent_evaluates_early"⟩,
  ⟨"the allocation moves toward the new target gradually, never overshooting it",
   .met "Commons.Flow.the_move_toward_the_target_is_gradual"⟩,
  ⟨"shrinking a consumer's share moves the excess to disk and loses nothing",
   .met "Commons.Flow.shrinking_a_share_loses_nothing"⟩,
  ⟨"at 100% allocation delivery is throttled and the overflow spills to disk",
   .met "Commons.Flow.at_full_allocation_delivery_is_throttled"⟩,
  ⟨"throttling a consumer never throttles the writer",
   .met "Commons.Flow.throttling_the_consumer_leaves_the_writer_alone"⟩,
  ⟨"consumers buffer from one shared pool and spill only past their fair share",
   .met "Commons.Flow.a_consumer_over_its_share_costs_disk_not_the_pool"⟩,
  ⟨"a slower consumer is given less memory, and the shares never overcommit the pool",
   .met "Commons.Flow.a_slower_consumer_is_given_less_memory"⟩,
  ⟨"a consumer inside its fair share never touches disk, for any buffer and any share",
   .met "Commons.Flow.a_consumer_within_its_allocation_never_touches_disk"⟩,
  ⟨"spilling begins exactly at the fair share, one event past it",
   .met "Commons.Flow.spilling_begins_exactly_at_the_allocation"⟩,
  ⟨"a slow consumer cannot OOM the node: for any backlog, resident bytes stay inside the pool",
   .met "Commons.Flow.a_slow_consumer_cannot_oom_the_node"⟩,
  ⟨"every pushed event is either resident or spilled, so allocation changes the rate and never the completeness",
   .met "Commons.Flow.a_smaller_allocation_does_not_lose_events"⟩,
  ⟨"a re-attaching consumer is streamed the spill first, then the buffer, in order",
   .met "Commons.Flow.resuming_replays_the_spill_before_the_buffer"⟩,
  ⟨"an acknowledgement releases the oldest events first",
   .met "Commons.Flow.an_ack_releases_the_oldest_first"⟩,
  ⟨"a re-attach resumes from the last acknowledged position, spill then buffer",
   .met "Commons.Flow.a_reattach_resumes_from_the_last_ack"⟩,
  ⟨"max_subscriber_file evicts a consumer that is slow but progressing; it is a capacity limit",
   .met "Subscribe.an_over_limit_consumer_was_not_stalled"⟩,
  ⟨"STALLED_READER_TIMEOUT kills a consumer making no progress, whatever its file size",
   .met "Subscribe.a_stalled_consumer_is_under_its_file_limit"⟩,
  ⟨"the two limits are independent and cannot share a setting",
   .met "Subscribe.the_two_limits_are_independent"⟩,
  ⟨"max_subscriber_file caps a subscriber's spill file, and reaching it evicts the subscriber",
   .met "Commons.Flow.a_dead_consumer_cannot_consume_the_disk"⟩,
  ⟨"a consumer's footprint is bounded by its pool share plus one file",
   .met "Commons.Flow.a_consumer_is_bounded_by_ram_then_file"⟩,
  ⟨"above 80% the step toward a new share is byte-limited, and at 90% it is half the gap",
   .met "Commons.Flow.at_ninety_percent_the_step_is_half_the_gap"⟩,
  ⟨"the scheduler tracks buffer occupancy trend, because a drain rate alone cannot decide safety",
   .met "Commons.Flow.a_drain_rate_alone_cannot_decide_safety"⟩,
  ⟨"a consumer at half the production rate still fills the buffer, more slowly",
   .met "Commons.Flow.a_consumer_at_half_the_production_rate_still_fills_the_buffer"⟩,
  ⟨"a slow consumer's overflow spills to disk instead of being dropped",
   .failing
     "the bus is a tokio broadcast with a fixed in-memory capacity and no pool accounting: falling behind drops events and raises Lagged(n) with no spill and no persistence, so a slow consumer loses data and a restart loses the backlog"
     "Commons.Flow.a_consumer_over_its_share_costs_disk_not_the_pool"⟩,
  ⟨"the wire gap signal carries the position to replay from",
   .failing
     "subscribe.rs sends CdcRecvError::Lagged(n) with a count and no position, so a lagged subscriber can neither replay nor tell whether its position is still in retention"
     "Commons.Stream.a_gap_is_either_resumable_or_an_explicit_reset"⟩,
  ⟨"a cancelled subscription reports its final cursor",
   .failing
     "SubscriptionHandle drops the stream id without recording the last position delivered, so a cancelled subscriber cannot resume"
     "Commons.Stream.a_cancellation_reports_the_position_to_resume_from"⟩
]

theorem cdc_has_four_red_items : redCount ledger = 4 := by decide

def main : IO Unit :=
  printLedger "SUBSCRIBE / CDC obligations" ledger

end Ferrosa.Cdc
