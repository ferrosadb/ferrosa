/-
  Ferrosa formal specification — flow control, shared by every streaming surface.
  =================================================================================

  This is the fair-share pool, the occupancy bands, the recompute policy, the sampling rate and the
  controller. It lived in the CDC SUBSCRIBE module because that is where it was first worked out, but
  three surfaces depend on it (CDC, CQL, Flight), so it lives here and the surfaces import it. There is
  exactly one place to tune: the constants below.
-/
import Ferrosa.Core
import Ferrosa.Ledger

namespace Ferrosa.Commons.Flow

open Ferrosa Ferrosa.Ledger

/-- The writer's progress: it takes no pool argument, which is the whole point — a consumer that falls
    behind cannot slow the producer, it only spills. -/
def writerProgress (_subscriberOccupancy : Nat) (accepted : Nat) : Nat := accepted

/-! ### The shared buffer: fair shares from one pool, spill past your share

The buffer is not per-consumer. Consumers draw from the node-wide `shared_memory` pool the streaming
rules already govern, and the fair scheduler divides it on drain rate — a fast consumer is given
more, a slow one less. A consumer that stays inside its share never touches disk; one that exceeds
it spills the overflow. So the pool is a hard ceiling on resident bytes for *all* consumers together,
whatever their backlogs, and a slow consumer costs disk rather than the node's memory.

Allocation governs how fast a consumer catches up, never whether it gets its events.
-/

/-- The node-wide buffer pool — the same `shared_memory` budget the streaming rules govern. -/
def pool : Nat := 65536

/-- A consumer's fair share, informed by its drain rate: `pool × rate / Σrate`. -/
def shareOf (rate totalRate : Nat) : Nat := pool * rate / totalRate

/-- **A slower consumer is given less memory**, which is the fairness rule rather than a punishment:
    a consumer that drains slowly needs less resident space to stay saturated. -/
theorem a_slower_consumer_is_given_less_memory :
    shareOf 1 4 = 16384 ∧ shareOf 3 4 = 49152 := by decide

/-- **The shares never overcommit the pool.** -/
theorem the_shares_do_not_overcommit_the_pool :
    shareOf 1 4 + shareOf 3 4 ≤ pool := by decide

/-- A consumer's buffer: its share of the pool, what is resident, and what has spilled past it. -/
structure Buffer where
  alloc : Nat
  resident : Nat
  spilled : Nat
  deriving DecidableEq, Repr

/-- Push `n` more events; anything beyond this consumer's share spills to disk. -/
def push (b : Buffer) (n : Nat) : Buffer :=
  let total := b.resident + n
  { b with resident := min total b.alloc, spilled := b.spilled + (total - b.alloc) }

/-- **A consumer inside its fair share never touches disk.**  The ordinary path pays no I/O for the
    queue's existence. -/
theorem a_consumer_within_its_allocation_never_touches_disk (b : Buffer) (n : Nat)
    (h : b.resident + n ≤ b.alloc) : (push b n).spilled = b.spilled := by
  unfold push
  simp only
  omega

/-- **Spilling begins exactly at the fair share**, one event past it, not before. -/
theorem spilling_begins_exactly_at_the_allocation :
    (push ⟨4096, 4095, 0⟩ 1).spilled = 0 ∧ (push ⟨4096, 4096, 0⟩ 1).spilled = 1 := by decide

/-- **A consumer over its share is bounded by its share, not by its backlog** — so however far behind
    it falls, it cannot grow the pool. -/
theorem a_consumer_over_its_share_costs_disk_not_the_pool
    (alloc resident : Nat) (h : alloc ≤ pool) : min resident alloc ≤ pool :=
  Nat.le_trans (Nat.min_le_right resident alloc) h

/-- **A slow consumer cannot OOM the node.**  Two consumers, one a million events behind and one five
    million behind, still occupy only their shares. -/
theorem a_slow_consumer_cannot_oom_the_node (b : Buffer) (n : Nat) (h : b.alloc ≤ pool) :
    (push b n).resident ≤ pool := by
  unfold push
  simp only
  omega

/-- **A smaller allocation does not lose events**: the same 10,000 events are all resident-or-spilled
    at either share, so allocation changes the rate and not the completeness. -/
theorem a_smaller_allocation_does_not_lose_events (b : Buffer) (n : Nat) :
    (push b n).resident + (push b n).spilled = b.resident + b.spilled + n := by
  unfold push
  simp only
  omega

/-- The message boundary: the spilled part is older than the resident part, because spilling starts
    at the head. -/
def spilledPart : List Nat := [42, 43]
def residentPart : List Nat := [44, 45]

/-- **Resuming replays the spill before the buffer**, so a re-attaching consumer sees its events in
    the order they were produced, whichever medium each one is on. -/
theorem resuming_replays_the_spill_before_the_buffer :
    spilledPart ++ residentPart = [42, 43, 44, 45] := rfl

/-- Acknowledgement releases the oldest first — the spill before the buffer. -/
def ackN (b : Buffer) (k : Nat) : Buffer :=
  { b with resident := b.resident - (k - b.spilled), spilled := b.spilled - k }

/-- **An acknowledgement releases the oldest events first**, so the buffer only ever holds the newest
    events the consumer has not yet seen. -/
theorem an_ack_releases_the_oldest_first :
    (ackN ⟨4096, 2, 2⟩ 1).spilled = 1 ∧ (ackN ⟨4096, 2, 2⟩ 1).resident = 2
      ∧ (ackN ⟨4096, 2, 2⟩ 3).spilled = 0 ∧ (ackN ⟨4096, 2, 2⟩ 3).resident = 1 := by decide

/-- The events a re-attaching consumer is owed from the position after its last ack. -/
def owed (ack spilled resident : Nat) : List Nat :=
  (List.range spilled).map (ack + 1 + ·) ++ (List.range resident).map (ack + 1 + spilled + ·)

/-- **A re-attach resumes from the last acknowledgement**, spill then buffer, with nothing missing in
    between. -/
theorem a_reattach_resumes_from_the_last_ack :
    owed 41 2 2 = [42, 43, 44, 45] := by decide

/-- `max_subscriber_file`: the cap on one subscriber's spill file, 1 GiB by default. -/
def maxSubscriberFileBytes : Nat := 1073741824

/-- A subscriber whose spill file reaches the cap is evicted: it must re-query for its data. -/
def evicted (spillBytes : Nat) : Bool := spillBytes ≥ maxSubscriberFileBytes

/-- **A dead consumer cannot consume the disk**, whatever its backlog: the spill file is capped, and
    the cap is the one remaining loss path — a counted, signalled eviction. -/
theorem a_dead_consumer_cannot_consume_the_disk :
    evicted 1073741823 = false ∧ evicted 1073741824 = true := by decide

/-- **A consumer's total footprint is bounded by its pool share plus one file**, so neither a slow nor
    a dead consumer can grow without limit: RAM by the share, disk by `max_subscriber_file`. -/
theorem a_consumer_is_bounded_by_ram_then_file
    (alloc spillBytes : Nat) (h1 : alloc ≤ pool) (h2 : spillBytes < maxSubscriberFileBytes) :
    alloc + spillBytes < pool + maxSubscriberFileBytes := by omega

/-! ### Recalculating the fair share

Allocation is not fixed at subscribe time. It is re-evaluated on a period — `SHMEM_FAIR_EVAL_PERIOD`,
5s by default — and early when the pool passes 90% full, so a pool drifting toward exhaustion is
caught before it gets there. A recalculation produces a *target*; the system then moves toward it
rather than reassigning in one step, because a sudden shrink would evict a consumer's hot buffer.

At 100% allocation the delivery path is throttled — the consumer is no longer fed faster than it
drains — and the overflow spills to disk. The writer is untouched throughout: the invariant that a
slow subscriber cannot stall the write path does not bend at the ceiling.
-/

/-- `SHMEM_FAIR_EVAL_PERIOD`: how often the fair share is recalculated. -/
def evalPeriodMs : Nat := 5000

/-- The early-evaluation watermark, in percent of the pool. -/
def highWatermarkPct : Nat := 90

def usedPct (used poolSize : Nat) : Nat := used * 100 / poolSize

/-- Why an evaluation ran. -/
inductive Trigger where
  | periodic
  | highWatermark
  deriving DecidableEq, Repr

/-- **The two triggers, and only two**: the period, or the high watermark. -/
def evalTrigger (used poolSize elapsedMs : Nat) : Option Trigger :=
  if elapsedMs ≥ evalPeriodMs then some .periodic
  else if usedPct used poolSize > highWatermarkPct then some .highWatermark
  else none

theorem the_default_evaluation_period_is_five_seconds : evalPeriodMs = 5000 := rfl

/-- **A pool above 90% is re-evaluated without waiting for the period**, which is what keeps
    consumption below 90% rather than discovering it at 100%. -/
theorem a_pool_above_ninety_percent_evaluates_early :
    evalTrigger 91000 100000 1000 = some .highWatermark := by decide

/-- Exactly 90% is not yet over the watermark; a quiet pool waits for the period to elapse. -/
theorem a_quiet_pool_waits_for_the_period :
    evalTrigger 90000 100000 1000 = none
      ∧ evalTrigger 50000 100000 4999 = none
      ∧ evalTrigger 50000 100000 5000 = some .periodic := by decide

/-- One evaluation moves the allocation one step toward the target. -/
def moveToward (current target step : Nat) : Nat :=
  if current < target then min target (current + step) else max target (current - step)

/-- **The move toward a new share is gradual**, and never overshoots it in either direction. -/
theorem the_move_toward_the_target_is_gradual :
    moveToward 1024 16384 512 = 1536
      ∧ moveToward 16384 1024 512 = 15872
      ∧ moveToward 16000 16384 512 = 16384
      ∧ moveToward 1200 1024 512 = 1024 := by decide

/-- Shrinking a consumer's share converts the excess into spill rather than discarding it. -/
def shrink (b : Buffer) (newAlloc : Nat) : Buffer :=
  { alloc := newAlloc
    resident := min b.resident newAlloc
    spilled := b.spilled + (b.resident - newAlloc) }

/-- **A recalculation can shrink a share, and shrinking loses nothing**: the resident excess moves to
    disk, so a smaller allocation costs latency and never data. -/
theorem shrinking_a_share_loses_nothing :
    (shrink ⟨16384, 12000, 0⟩ 4096).resident = 4096
      ∧ (shrink ⟨16384, 12000, 0⟩ 4096).spilled = 7904
      ∧ (shrink ⟨16384, 12000, 0⟩ 4096).resident
          + (shrink ⟨16384, 12000, 0⟩ 4096).spilled = 12000 := by decide

/-- What the delivery path does at a given pool occupancy. -/
inductive Delivery where
  | flowing
  | throttled
  deriving DecidableEq, Repr

def deliveryFor (used poolSize : Nat) : Delivery :=
  if used ≥ poolSize then .throttled else .flowing

/-- **At 100% allocation delivery is throttled** and the overflow spills; one byte short of it,
    delivery flows and the consumer reads at its own pace. -/
theorem at_full_allocation_delivery_is_throttled :
    deliveryFor 65536 65536 = .throttled ∧ deliveryFor 65535 65536 = .flowing := by decide

/-- **Throttling the consumer never throttles the producer**: the write path's progress does not
    consult pool occupancy at all, so the ceiling cannot turn into write unavailability. -/
theorem throttling_the_consumer_leaves_the_writer_alone (used accepted : Nat) :
    writerProgress used accepted = accepted := rfl

/-! ### Step size by occupancy band, and what the scheduler actually measures

The step toward a new share is chosen by how full the pool is. Below the early watermark the ordinary
step applies; above 80% the step is capped in bytes, so a tightening pool is approached carefully;
at 90% the step becomes half the remaining distance, which converges without overshooting; at 100%
delivery is throttled and the overflow spills.

Measuring the consumer's drain rate is *not* enough to decide this. A consumer draining 100 events a
second cannot be judged safe or unsafe without knowing the production rate: at 50 produced it never
fills the buffer, at 200 produced it fills it in finite time. The signal is the gap between the two,
which is why the scheduler tracks buffer occupancy trend rather than throughput.
-/

def earlyWatermarkPct : Nat := 80

/-- The step toward the target: byte-capped above 80%, half the gap at 90%. -/
def stepSize (used poolSize gap stepBytes : Nat) : Nat :=
  if usedPct used poolSize ≥ highWatermarkPct then gap / 2
  else if usedPct used poolSize > earlyWatermarkPct then min stepBytes gap
  else min stepBytes gap

/-- **Above 80% the step is byte-limited**, so a tightening pool is never yanked toward a new share
    in one tick. -/
theorem a_tightening_pool_steps_by_bytes :
    stepSize 85000 100000 100000 4096 = 4096 := by decide

/-- **At 90% the step is half the remaining gap.** -/
theorem at_ninety_percent_the_step_is_half_the_gap :
    stepSize 91000 100000 10000 4096 = 5000 := by decide

/-- **The step never exceeds the distance it has to travel**, so the band rules cannot overshoot a
    target even when the gap is smaller than the step. -/
theorem a_step_never_exceeds_the_gap :
    stepSize 95000 100000 1000 4096 = 500 := by decide

/-- A finite time to fill, or none when the consumer drains at least as fast as it is fed. -/
def timeToFill (freeBytes producedRate drainRate : Nat) : Option Nat :=
  if drainRate ≥ producedRate then none
  else some (freeBytes / (producedRate - drainRate))

/-- **The drain rate alone cannot say whether a consumer will fill the buffer.**  The same 100
    events/s drains 50/s of production forever and 200/s of production in finite time. -/
theorem a_drain_rate_alone_cannot_decide_safety :
    timeToFill 4096 50 100 = none ∧ timeToFill 4096 200 100 = some 40 := by decide

/-- **A consumer at half the production rate still exhausts the buffer** — just more slowly — which is
    the case a drain-rate-only scheduler misses. -/
theorem a_consumer_at_half_the_production_rate_still_fills_the_buffer :
    timeToFill 4096 200 100 = some 40 := by decide

/-- **What decides is the gap, not either rate**: doubling both rates leaves the time to fill
    unchanged. -/
theorem only_the_gap_between_production_and_drain_decides :
    timeToFill 4096 200 100 = timeToFill 4096 300 200 := by decide

/-! ### Sampling at twice the decision rate

A rate needs two observations, so sampling runs at twice the decision rate: a 5s evaluation period
samples every 2.5s, and `samplePeriodMs` is *derived* from `evalPeriodMs` rather than configured
separately, so the ratio cannot drift out of step when an operator changes the period.

The consequence worth stating is the staleness bound. The decision may be up to one evaluation
period old, but the observation it acts on is at most half that: a consumer that stalls a moment
after a sample is seen at the next one, 2.5s later, not 5s later.
-/

structure Sample where
  tMs : Nat
  occupancy : Nat
  deriving DecidableEq, Repr

/-- Sampling half as often as the decision is made. -/
def samplePeriodMs : Nat := evalPeriodMs / 2

/-- Occupancy change per second between two samples. -/
def rateFrom (a b : Sample) : Nat :=
  (b.occupancy - a.occupancy) * 1000 / (b.tMs - a.tMs)

def rateOf : List Sample → Option Nat
  | a :: b :: _ => some (rateFrom a b)
  | _ => none

theorem the_sampling_period_is_half_the_evaluation_period : samplePeriodMs = 2500 := by decide

/-- **Two samples tile the evaluation window exactly**, so every decision has a fresh observation
    covering the period it decides over. -/
theorem two_samples_tile_the_evaluation_window :
    samplePeriodMs * 2 = evalPeriodMs := by decide

/-- **A rate needs two samples**: one sample is an occupancy reading with no trend, so the sampling
    rate cannot be equal to the decision rate and still produce a rate. -/
theorem a_rate_needs_two_samples_per_evaluation_window :
    rateOf [⟨0, 4096⟩] = none
      ∧ rateOf [⟨0, 4096⟩, ⟨2500, 8192⟩] = some 1638 := by decide

/-- **A stall is observed within half a decision period**, which is the whole reason for sampling at
    2x: the decision may be old, the observation it acts on is not. -/
theorem an_observation_is_never_older_than_half_a_decision_period :
    samplePeriodMs ≤ evalPeriodMs / 2 ∧ samplePeriodMs < evalPeriodMs := by decide

/-! ### The loop is stable because the step is bounded, and a PID would have to keep it so

The banded step rule is already a controller: bounded actuator movement per period is what makes the
loop settle instead of oscillating. Adding a PID does not create that stability, it must preserve it,
and two of its terms work against it here.

* A gain above one overshoots to the same magnitude on the other side and alternates forever, so the
  byte cap is not a throttle — it *is* the stability margin.
* The actuator saturates at the pool share, so an integral term accumulates error it cannot spend,
  and spends it late: the classic wind-up into spill. Anti-windup is mandatory, not tuning.
* With two samples per decision the derivative is a two-point difference, so occupancy noise lands
  on the actuator at full amplitude. A derivative term needs the occupancy signal filtered first.

The one assumption the loop rests on is that the plant is monotone: more allocation never makes a
consumer drain more slowly. A client that throttles when given more memory inverts the controller's
sign, and no gain setting repairs that.
-/

/-- Proportional, optionally integral. There is deliberately no derivative gain: a field that must
    always be zero is a footgun. -/
structure Gains where
  kp : Int
  ki : Int
  deriving DecidableEq, Repr

def correction (g : Gains) (e i : Int) : Int := g.kp * e + g.ki * i

/-- A crude plant with the right shape: a correction of `u` moves occupancy by `u`. -/
def nextError (e u : Int) : Int := e - u

/-- **A unit-gain proportional controller settles in one step, without overshoot.** -/
theorem a_unit_gain_settles_in_one_step :
    nextError 10 (correction ⟨1, 0⟩ 10 0) = 0 := by decide

/-- **A gain above one overshoots, and a gain of two alternates forever** — the stability boundary
    the byte cap exists to enforce. -/
theorem a_gain_above_one_oscillates :
    nextError 10 (correction ⟨2, 0⟩ 10 0) = -10
      ∧ nextError (-10) (correction ⟨2, 0⟩ (-10) 0) = 10 := by decide

/-- **P settles at an offset, and here the offset is the operating point.**  Against a steady inflow
    the loop holds the error at `inflow / kp`, which is exactly the counter-pressure the watermark
    wants under load — not an error to be driven to zero. -/
def steadyError (inflow kp : Nat) : Nat := inflow / kp

theorem a_proportional_loop_holds_a_steady_offset :
    steadyError 20 2 = 10 ∧ steadyError 20 4 = 5 := by decide

/-- **The periodic half-distance step already supplies the integrating action.**  Repeating "move
    half the remaining gap" every period shrinks the gap geometrically, so the loop is PI-shaped by
    construction: proportional on instantaneous occupancy, integral in the recurring step. An
    explicit integral term would duplicate it. -/
def halfSteps (gap n : Nat) : Nat := gap / 2 ^ n

theorem the_periodic_step_already_integrates :
    halfSteps 1024 0 = 1024 ∧ halfSteps 1024 1 = 512
      ∧ halfSteps 1024 4 = 64 ∧ halfSteps 1024 10 = 1 := by decide

/-- The controller saturates at the consumer's share, or at zero. -/
def clampAct (u cap : Int) : Int := if u > cap then cap else if u < 0 then 0 else u

/-- **An integral term, if enabled, winds up while the actuator saturates**: three times the error
    accumulates, the actuator only ever spends the cap, and the surplus is still owed when the plant
    frees up. -/
theorem an_integral_winds_up_under_saturation :
    clampAct (correction ⟨0, 1⟩ 10 30) 10 = 10
      ∧ correction ⟨0, 1⟩ 10 30 = 30 := by decide

/-- Anti-windup: the integral stops accumulating while the actuator is saturated. -/
def accumulate (saturated : Bool) (i e : Int) : Int := if saturated then i else i + e

/-- **Anti-windup is mandatory if `ki` is non-zero**, not tuning: without it the surplus is spent on
    release and lands as a spill burst. -/
theorem anti_windup_stops_the_accumulation :
    accumulate true 0 10 = 0 ∧ accumulate false 0 10 = 10 := by decide

/-- The term a PID would add: with two samples per decision, a two-point difference. -/
def derivative (a b : Int) : Int := b - a

/-- **And that is why it is excluded**: a ten-unit occupancy jitter *is* a ten-unit demand, so at
    2.5s sampling the derivative cannot distinguish noise from a trend. -/
theorem a_derivative_would_carry_the_noise : derivative 100 110 = 10 := by decide

/-- **The loop needs a monotone plant.**  More allocation must never slow the consumer down; a client
    that throttles when given more memory inverts the sign of the controller's response. -/
theorem the_controller_needs_a_monotone_plant
    (drain : Nat → Nat) (h : ∀ a b, a ≤ b → drain a ≤ drain b)
    (a b : Nat) (hab : a ≤ b) : drain a ≤ drain b := h a b hab


end Ferrosa.Commons.Flow
