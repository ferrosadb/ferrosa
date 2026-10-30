/-
  Ferrosa formal specification — audit of `specs/tla/multi-dc.tla`.
  ===============================================================

  `specs/tla/multi-dc.tla` claims to model independent per-DC Raft groups under a cross-DC
  Accord layer, and it names four invariants; encoding the actions it actually has shows
  that three of them cannot fail and the fourth does.  The three vacuous ones: no action
  assigns `pendingConfig`, so the DC-swap drain has an antecedent that never holds;
  `appliedAccord` is a set that only ever gains committed transactions, so applying a
  transaction twice is not expressible and the idempotence invariant restates the guards;
  and commit and abort each guard on the other's set, so their mutual exclusion is an
  invariant of the guards rather than of the protocol.  The one that can fail is
  timestamp-ordered apply — commit accepts any HLC with no reference to the datacenter's
  watermark, so a transaction can be committed at a timestamp the datacenter has already
  applied past, and the apply order then disagrees with commit-timestamp order.  The per-DC
  Raft layer is a stub: no action advances a commit index, so no Raft property of the
  composition is checked at all.
-/
import Ferrosa.Core

namespace Ferrosa.Accord.AccordMultiDcLean

open Ferrosa

structure Accord where
  /-- Per-DC HLC watermark. -/
  watermark : Nat
  /-- Committed transactions with their commit timestamps, in commit order. -/
  buffer : List (Nat × Nat)
  /-- Transactions already applied, in apply order. -/
  applied : List Nat
  committed : List Nat
  aborted : List Nat
  /-- Per-DC membership during a joint-consensus DC swap.  **Never assigned by
      any action of `multi-dc.tla`.** -/
  pending : List (List Nat)
  /-- Per-DC Raft commit index.  **Never advanced by any action either.** -/
  commitIdx : List Nat
  deriving DecidableEq, Repr

def init : Accord where
  watermark := 0
  buffer := []
  applied := []
  committed := []
  aborted := []
  pending := [[], []]
  commitIdx := [0, 0]

/-- The transactions the watermark lets through: committed, buffered, and within
    `w`. -/
def toApply (s : Accord) (w : Nat) : List Nat :=
  (s.buffer.filter (fun p => p.2 ≤ w && s.committed.contains p.1)).map (fun p => p.1)

inductive Step : Accord → Accord → Prop where
  /-- `AccordCommit(t, hlc)` — note the absent guard on `hlcWatermark`. -/
  | commit {s : Accord} {t hlc : Nat}
      (hnotCommitted : t ∉ s.committed) (hnotAborted : t ∉ s.aborted)
      (hbound : hlc ≤ 10) :
      Step s { s with buffer := s.buffer ++ [(t, hlc)], committed := t :: s.committed }
  /-- `AdvanceWatermark(d, w)` — monotone, drains everything at or below `w`. -/
  | advance {s : Accord} {w : Nat} (hmono : s.watermark ≤ w) (hbound : w ≤ 10) :
      Step s { s with watermark := w,
                      applied := s.applied ++ (toApply s w).filter (fun t => !s.applied.contains t) }
  /-- `AccordAbort(t)`. -/
  | abort {s : Accord} {t : Nat}
      (hnotCommitted : t ∉ s.committed) (hnotAborted : t ∉ s.aborted) :
      Step s { s with aborted := t :: s.aborted }

inductive Reach : Accord → Prop where
  | init : Reach init
  | step : Reach s → Step s s' → Reach s'

/-! ## The one invariant that can fail: apply order -/

/-- Commit timestamp of `t`, if it is in the buffer. -/
def tsOf (s : Accord) (t : Nat) : Option Nat :=
  (s.buffer.find? (fun p => p.1 == t)).map (fun p => p.2)

/-- `true` when some adjacent pair in `applied` has a *decreasing* commit
    timestamp — i.e. the apply order disagrees with commit-timestamp order. -/
def appliedOutOfOrder (s : Accord) : Bool :=
  (s.applied.zip s.applied.tail).any (fun p =>
    match tsOf s p.1, tsOf s p.2 with
    | some a, some b => b < a
    | _, _ => false)

/-- **A transaction can be committed below the DC's watermark.** -/
theorem stale_commit_timestamp_reachable :
    ∃ s, Reach s ∧ s.watermark = 5 ∧ (2, 3) ∈ s.buffer := by
  let s1 : Accord := { init with buffer := [(1, 5)], committed := [1] }
  let s2 : Accord := { s1 with watermark := 5, applied := [1] }
  let s3 : Accord := { s2 with buffer := [(1, 5), (2, 3)], committed := [2, 1] }
  have h1 : Reach s1 := Reach.step Reach.init
    (Step.commit (t := 1) (hlc := 5) (by simp [init]) (by simp [init]) (by decide))
  have h2 : Reach s2 := Reach.step h1
    (Step.advance (w := 5) (by simp [s1, init]) (by decide))
  have h3 : Reach s3 := Reach.step h2
    (Step.commit (t := 2) (hlc := 3) (by simp [s2, s1, init]) (by simp [s2, s1, init]) (by decide))
  exact ⟨s3, h3, by simp [s3, s2], by simp [s3]⟩

/-- **I-27 fails in the module as written.**  Transaction 1 is committed at HLC 5
    and applied; transaction 2 is then committed at HLC 3 — below the watermark —
    and applied afterwards.  The apply order is `[1, 2]` while the commit-ts
    order is `2, 1`. -/
theorem apply_order_violation :
    ∃ s, Reach s ∧ appliedOutOfOrder s = true ∧ s.applied = [1, 2] := by
  let s1 : Accord := { init with buffer := [(1, 5)], committed := [1] }
  let s2 : Accord := { s1 with watermark := 5, applied := [1] }
  let s3 : Accord := { s2 with buffer := [(1, 5), (2, 3)], committed := [2, 1] }
  let s4 : Accord := { s3 with applied := [1, 2] }
  have h1 : Reach s1 := Reach.step Reach.init
    (Step.commit (t := 1) (hlc := 5) (by simp [init]) (by simp [init]) (by decide))
  have h2 : Reach s2 := Reach.step h1
    (Step.advance (w := 5) (by simp [s1, init]) (by decide))
  have h3 : Reach s3 := Reach.step h2
    (Step.commit (t := 2) (hlc := 3) (by simp [s2, s1, init]) (by simp [s2, s1, init]) (by decide))
  have h4 : Reach s4 := Reach.step h3
    (Step.advance (w := 5) (by simp [s3, s2, s1, init]) (by decide))
  refine ⟨s4, h4, ?_, by simp [s4]⟩
  simp [appliedOutOfOrder, tsOf, s4, s3]

/-! ## The three invariants that cannot fail, and what that means for the audit -/

/-- **I-30 is vacuous**: no action of the module assigns `pendingConfig`. -/
theorem swap_dc_antecedent_never_holds :
    ∀ s, Reach s → s.pending = [[], []] := by
  intro s h
  induction h with
  | init => rfl
  | step _ hs ih =>
      cases hs <;> simp_all

/-- Anything the watermark lets through is committed. -/
theorem mem_toApply_committed {s : Accord} {w t : Nat} (h : t ∈ toApply s w) :
    s.committed.contains t = true := by
  unfold toApply at h
  rw [List.mem_map] at h
  obtain ⟨p, hp, rfl⟩ := h
  simp only [List.mem_filter, Bool.and_eq_true] at hp
  exact hp.2.2

/-- **I-28 holds by construction**: `appliedAccord` is a set that only ever gains
    committed transactions, so "applied an aborted transaction" is unreachable
    without any idempotence argument. -/
theorem applied_implies_committed :
    ∀ s, Reach s → ∀ t ∈ s.applied, t ∈ s.committed := by
  intro s h
  induction h with
  | init => intro t ht; simp [init] at ht
  | step hr hs ih =>
      intro t ht
      cases hs with
      | commit _ _ _ =>
          simp only [List.mem_cons] at ht ⊢
          exact Or.inr (ih t ht)
      | advance _ _ =>
          simp only [List.mem_append, List.mem_filter] at ht ⊢
          rcases ht with h1 | h2
          · exact ih t h1
          · have hc := mem_toApply_committed h2.1
            rwa [List.contains_iff_mem] at hc
      | abort _ _ =>
          simp only [List.mem_cons] at ht ⊢
          exact ih t ht

/-- **The per-DC Raft layer is a stub**: nothing in the module advances a commit
    index, so no Raft property of the multi-DC composition is checked. -/
theorem per_dc_commit_index_never_advances :
    ∀ s, Reach s → s.commitIdx = [0, 0] := by
  intro s h
  induction h with
  | init => rfl
  | step _ hs ih =>
      cases hs <;> simp_all

/-- **Watermark monotonicity is a guard, not a theorem**: `AdvanceWatermark`
    requires `w ≥ hlcWatermark[d]`, so `[][watermark'[d] ≥ watermark[d]]` is
    true because the step is only enabled when it is.  Stated here explicitly so
    the invariant is not mistaken for evidence. -/
theorem watermark_monotone_by_guard {s s' : Accord} (h : Step s s')
    (hadv : s'.watermark ≠ s.watermark) : s.watermark ≤ s'.watermark := by
  cases h with
  | commit _ _ _ => simp at hadv
  | advance hmono _ => exact hmono
  | abort _ _ => simp at hadv

end Ferrosa.Accord.AccordMultiDcLean
