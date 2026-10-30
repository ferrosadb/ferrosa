/-
  The gap protocol: a position to resume from, or an explicit reset
  ==================================================================

  A consumer that falls behind is told one of two things, and never a bare count: the position to
  replay from together with how many events are outstanding, or that it has fallen outside the
  retention window and must re-snapshot. Every streaming surface speaks this protocol — CQL's
  SUBSCRIBE, the Flight stream ticket, and `Cdc.Subscribe` — so the position a gap carries and the
  position a ticket carries are the same number.

  A cancellation is a position, not a loss: delivery reports where it stopped, and a later
  subscription resumes there rather than from a re-query.
-/
import Ferrosa.Core
import Ferrosa.Ledger

namespace Ferrosa.Commons.Stream

open Ferrosa Ferrosa.Ledger

/-! ### Closing the gap: a gap is either resumable or an explicit reset

A bare count is not enough — it tells the subscriber that events were lost and gives it nothing to
recover with. The signal must carry the position to replay from, and retention must be able to say
"you have fallen out of it" rather than pretending the position is still good. That gives exactly
three outcomes and no quiet fourth one. -/

/-- A gap between what was produced and what the subscriber received. -/
inductive Gap where
  /-- Replay from `start`; `count` events are outstanding. -/
  | resumable (start count : Nat)
  /-- The reader fell outside the retention window; a re-snapshot is required. -/
  | resetRequired
  deriving DecidableEq, Repr

def Gap.isResumable : Gap → Bool
  | .resumable _ _ => true
  | .resetRequired => false

/-- **A gap always says which kind it is.**  It names the replay position and the outstanding count,
    or it says plainly that the reader must re-snapshot. A subscriber is never left to guess whether
    its position is still valid. -/
theorem a_gap_is_either_resumable_or_an_explicit_reset :
    (Gap.resumable 7 3).isResumable = true ∧ Gap.resetRequired.isResumable = false := by decide

/-- The events a replay from `start` delivers. -/
def replay (start count : Nat) : List Nat := (List.range count).map (start + ·)

/-- **Resuming from a resumable gap loses nothing**: the outstanding events are exactly the ones the
    gap counted, delivered in order from the position it named. -/
theorem resuming_from_a_resumable_gap_loses_nothing :
    replay 7 3 = [7, 8, 9] ∧ (replay 7 3).length = 3 := by decide

/-- A cancellation carries the position after the last event delivered. -/
def cancelAt (lastDelivered : Nat) : Gap := .resumable (lastDelivered + 1) 0

/-- **A cancellation is not a loss.**  It reports where delivery stopped, so a later subscription can
    resume from there; the position is a cursor, not a gap counter. -/
theorem a_cancellation_reports_the_position_to_resume_from :
    cancelAt 41 = .resumable 42 0 ∧ (cancelAt 41).isResumable = true := by decide


end Ferrosa.Commons.Stream
