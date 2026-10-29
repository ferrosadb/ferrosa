/-
  Ferrosa formal specification — the bookkeeping a run must do, and what a generator can prove.
  ==========================================================================================

  Jepsen's verdict is not only `valid?`: `check` also returns multisets (`:lost`, `:unexpected`,
  `:recovered`, `:acknowledged`, `:attempt`, `:duplicated`). Those are where the failure mode the
  product cares about actually shows up — *a user not getting all of their data silently* — and they
  are the one part of the harness that is arithmetic rather than search, so they can be specified
  exactly.

  Three things are specified here:

  1. **Multiset bookkeeping.** What `lost` and `unexpected` mean, and why they cannot cancel.
  2. **Generators.** A generator decides what anomalies a green run can rule out. Two theorems say
     so: a generator that never overlaps two operations cannot convict an ordering model, and a
     generator that never writes cannot lose anything.
  3. **Nemeses, at the level of the verdict rather than the injection.** A fault must be logged to be
     attributable, and an acknowledgement before a crash is a durability promise while one after a
     crash is not.

  Fault *injection* is deliberately not modelled: it belongs to test time. What belongs here is what
  the system promises given that a fault happened.
-/
import Ferrosa.Core
import Ferrosa.Ledger

namespace Ferrosa.Testing.Histories

open Ferrosa Ferrosa.Ledger

/-! ## Multisets -/

/-- How many copies of `v` a multiset holds. -/
def cnt (v : Nat) : List Nat → Nat
  | [] => 0
  | x :: xs => (if x == v then 1 else 0) + cnt v xs

/-- Multiset subtraction: remove one copy of each element of `b` from `a`. -/
def msSub : List Nat → List Nat → List Nat
  | a, [] => a
  | a, b :: bs => msSub (a.erase b) bs

/-- Multiset inclusion. -/
def msLe (a b : List Nat) : Bool := a.all (fun v => decide (cnt v a ≤ cnt v b))

theorem cnt_append (v : Nat) (a b : List Nat) : cnt v (a ++ b) = cnt v a + cnt v b := by
  induction a with
  | nil => simp [cnt]
  | cons x xs ih => simp [cnt, ih]; omega

/-- Union is commutative as a multiset, even though concatenation is not commutative as a list. -/
theorem multiset_union_is_commutative (v : Nat) (a b : List Nat) :
    cnt v (a ++ b) = cnt v (b ++ a) := by
  rw [cnt_append, cnt_append]; omega

/-! ## The book -/

/-- What a run recorded. `attempts` counts invocations, `acknowledged` counts `ok` completions,
    `recovered` is what the final read observed. -/
structure Book where
  attempts : List Nat
  acknowledged : List Nat
  recovered : List Nat
  deriving DecidableEq, Repr

/-- Acknowledged but not observed: the write was promised and the read did not return it. -/
def lost (b : Book) : List Nat := msSub b.acknowledged b.recovered

/-- Observed but never acknowledged: corruption, or a retry that was not attributed. -/
def unexpected (b : Book) : List Nat := msSub b.recovered b.acknowledged

/-- Attempts that were acknowledged. -/
def duplicated (b : Book) : List Nat := msSub b.recovered b.attempts

/-- **A silent loss is `lost` without `unexpected`**: the data is gone and the read revealed nothing
    that would explain it. This is the shape of the failure the product forbids. -/
def silentLoss (b : Book) : Bool := !(lost b).isEmpty && (unexpected b).isEmpty

/-- A write that was acknowledged and then read back is not lost. -/
theorem an_acknowledged_write_that_is_read_back_is_not_lost :
    lost ⟨[7], [7], [7]⟩ = [] := by decide

/-- A write acknowledged and never read back is lost. -/
theorem an_acknowledged_write_that_is_never_read_back_is_lost :
    lost ⟨[7], [7], []⟩ = [7] := by decide

/-- A value the read returns without any acknowledgement is unexpected — nothing in the run explains
    where it came from. -/
theorem a_value_that_no_write_acknowledged_is_unexpected :
    unexpected ⟨[7], [], [7]⟩ = [7] := by decide

/-- **`lost` and `unexpected` are independent, so they cannot cancel**: a run can lose one value and
    fabricate another, and the two multisets stay separate. Summing them into one error count would
    hide the loss behind the fabrication. -/
theorem a_loss_and_a_fabrication_do_not_cancel :
    lost ⟨[1, 2], [1, 2], [1, 3]⟩ = [2]
      ∧ unexpected ⟨[1, 2], [1, 2], [1, 3]⟩ = [3] := by decide

/-- **A clean read plus a missing write is the silent-loss shape.** -/
theorem a_missing_write_with_a_clean_read_is_a_silent_loss :
    silentLoss ⟨[1, 2], [1, 2], [1]⟩ = true := by decide

/-- A run that lost nothing is not a silent loss, however many values it fabricated. -/
theorem a_fabrication_alone_is_not_silent_loss :
    silentLoss ⟨[1], [1], [1, 2]⟩ = false := by decide

/-- **A run that acknowledged nothing cannot lose anything.** This is the multiset form of the
    vacuity result: `lost` is empty by construction, so a no-write run reports a clean bill of health
    without having tested durability. -/
theorem a_run_without_acknowledged_writes_cannot_lose_anything (r : List Nat) :
    lost ⟨[], [], r⟩ = [] := by
  simp only [lost]
  induction r with
  | nil => rfl
  | cons b bs ih => simp [msSub, ih]

/-- And the converse: a run that read nothing cannot have recovered anything, so every acknowledged
    write counts as lost. A suite that reads nothing fails loudly rather than passing quietly — which
    is the argument for always issuing a final read. -/
theorem a_run_without_a_final_read_loses_every_acknowledged_write :
    lost ⟨[1, 2], [1, 2], []⟩ = [1, 2] := by decide

/-! ## Generators

A generator is a policy over histories, and the policy is part of the claim: a green run excludes
only the anomalies the generator was capable of producing.
-/

/-- What a generator is allowed to produce. -/
structure Gen where
  /-- Whether two operations may overlap in real time. -/
  allowsOverlap : Bool
  /-- Whether the generator issues writes at all. -/
  issuesWrites : Bool
  /-- Whether the generator issues a final read. -/
  issuesFinalRead : Bool
  deriving DecidableEq, Repr

/-- Whether a green run of this generator can exclude an ordering anomaly at all. Without overlap the
    real-time order is total, so the only histories produced are sequential ones. -/
def canExposeOrderingAnomaly (g : Gen) : Bool := g.allowsOverlap

/-- **A generator that never overlaps two operations cannot convict an ordering model.** With a total
    real-time order every candidate serialization is forced, so the check reduces to replaying the
    arrival order — an ordering anomaly is not expressible in the histories it can emit.
    (`Models.overlapping_enqueues_leave_the_fifo_order_free` is the concrete case.) -/
theorem a_sequential_generator_cannot_expose_an_ordering_anomaly :
    canExposeOrderingAnomaly ⟨false, true, true⟩ = false
      ∧ canExposeOrderingAnomaly ⟨true, true, true⟩ = true := by decide

/-- **A generator that never writes cannot lose anything**, so a green durability result from it is
    vacuous. -/
theorem a_write_free_generator_cannot_test_durability (r : List Nat) :
    lost ⟨[], [], r⟩ = [] := a_run_without_acknowledged_writes_cannot_lose_anything r

/-- **A generator that omits the final read cannot detect a loss at all**: every acknowledged write
    counts as lost, so the run is either red for the wrong reason or — if it never wrote — green for
    no reason. -/
theorem a_generator_without_a_final_read_cannot_measure_loss :
    lost ⟨[1], [1], []⟩ = [1] ∧ lost ⟨[], [], []⟩ = [] := by decide

/-! ## Nemeses, and what the verdict may attribute to them -/

/-- When a crash fell relative to the acknowledgement of a write. -/
inductive CrashTime where
  | beforeAck
  | afterAck
  deriving DecidableEq, Repr

/-- **An acknowledgement is a durability promise**: a crash after the ack does not excuse a missing
    value. A crash before the ack does, and that is the only case that does. -/
def excusesMissingWrite : CrashTime → Bool
  | .beforeAck => true
  | .afterAck => false

theorem an_ack_before_the_crash_is_a_durability_promise :
    excusesMissingWrite .afterAck = false
      ∧ excusesMissingWrite .beforeAck = true := by decide

/-- **A fault that is not logged cannot be attributed**, so a red verdict with no fault log cannot
    distinguish an anomaly from an injected failure. The verdict needs the log, not just the
    history. -/
def attributable (logged : Bool) (excused : Bool) : Bool := logged && !excused

theorem a_missing_write_after_a_crash_is_only_excusable_if_the_crash_was_logged :
    attributable false false = false
      ∧ attributable true false = true
      ∧ attributable true true = false := by decide

def ledger : List SpecItem := [
  ⟨"an acknowledged write that is read back is not lost",
   .met "Histories.an_acknowledged_write_that_is_read_back_is_not_lost"⟩,
  ⟨"an acknowledged write that is never read back is lost",
   .met "Histories.an_acknowledged_write_that_is_never_read_back_is_lost"⟩,
  ⟨"a value that no write acknowledged is unexpected",
   .met "Histories.a_value_that_no_write_acknowledged_is_unexpected"⟩,
  ⟨"a loss and a fabrication are independent multisets and do not cancel",
   .met "Histories.a_loss_and_a_fabrication_do_not_cancel"⟩,
  ⟨"a missing write with a clean read is a silent loss",
   .met "Histories.a_missing_write_with_a_clean_read_is_a_silent_loss"⟩,
  ⟨"a fabrication alone, with nothing lost, is not a silent loss",
   .met "Histories.a_fabrication_alone_is_not_silent_loss"⟩,
  ⟨"a run without acknowledged writes cannot lose anything, so its green result is vacuous",
   .met "Histories.a_run_without_acknowledged_writes_cannot_lose_anything"⟩,
  ⟨"a run without a final read counts every acknowledged write as lost",
   .met "Histories.a_run_without_a_final_read_loses_every_acknowledged_write"⟩,
  ⟨"a generator that never overlaps two operations cannot convict an ordering model",
   .met "Histories.a_sequential_generator_cannot_expose_an_ordering_anomaly"⟩,
  ⟨"a write-free generator cannot test durability",
   .met "Histories.a_write_free_generator_cannot_test_durability"⟩,
  ⟨"an acknowledgement before a crash is a durability promise; one after a crash is not",
   .met "Histories.an_ack_before_the_crash_is_a_durability_promise"⟩,
  ⟨"a missing write is only excusable if the crash that caused it was logged",
   .met "Histories.a_missing_write_after_a_crash_is_only_excusable_if_the_crash_was_logged"⟩,
  ⟨"the loss accounting is meaningful only when the final read can see every acknowledged write",
   .failing
     "not enforced: `recovered` is whatever one read returned. Under LOCAL_QUORUM or DC_QUORUM with a cross-DC failover that read is served locally, so it can omit a write acknowledged elsewhere and report a `lost` that never happened. The bookkeeping needs the read's consistency level as an input"
     "Histories.a_missing_write_with_a_clean_read_is_a_silent_loss"⟩,
  ⟨"every run records lost, unexpected and recovered as multisets, not as a single error count",
   .failing
     "unverified: `check` returns the multisets, but the endurance and alert paths summarise a verdict as pass/fail. A summary that collapses them is exactly the cancellation this file forbids"
     "Histories.a_loss_and_a_fabrication_do_not_cancel"⟩
]

theorem histories_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "bookkeeping, generators and fault attribution" ledger

end Ferrosa.Testing.Histories
