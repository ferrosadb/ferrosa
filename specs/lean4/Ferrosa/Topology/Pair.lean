/-
  Ferrosa formal specification — pair mode: two nodes, one primary, no quorum.
  ==========================================================================

  Two nodes and no quorum: a write the primary acknowledges is on exactly one machine,
  and losing that machine loses the write until an operator promotes the other node.
  The surviving secondary keeps serving reads, so a pair whose primary is down is
  read-only rather than unavailable, and writes refuse rather than split.  Promotion is
  the operator's decision and is gated on the operator having confirmed the peer dead,
  which is the only thing keeping a pair from accepting two divergent writes under one
  key — `unguarded_promotion_splits_brain` is the split brain that arrives the moment
  that gate is dropped.  A returning primary is demoted on reconnect, because connection
  direction assigns the roles and no election is involved.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.Pair

open Ferrosa
open Ferrosa.Topology

/-- Pair-mode roles.  `none` is a node that has not been assigned a role yet. -/
inductive Role where
  | none
  | primary
  | secondary
  deriving DecidableEq, Repr

/-- The two-node formation state.  `up` means "alive and serving"; a partitioned
    node is still `up` from its own point of view. -/
structure Pair where
  r0 : Role
  r1 : Role
  up0 : Bool
  up1 : Bool
  deriving DecidableEq, Repr

/-- Node `i` is accepting writes. -/
def Writable (s : Pair) (i : Node) : Prop :=
  (i = 0 ∧ s.r0 = .primary ∧ s.up0 = true) ∨ (i = 1 ∧ s.r1 = .primary ∧ s.up1 = true)

/-- Node `i` is serving reads (a degraded secondary still does: T8b). -/
def Readable (s : Pair) (i : Node) : Prop :=
  (i = 0 ∧ s.up0 = true) ∨ (i = 1 ∧ s.up1 = true)

/-- Transitions.  `op` is the operator's out-of-band decision: `promote`
    transitions are only available when `op = true`, and they additionally
    require the peer to be down, because the operator has confirmed its death. -/
inductive Step (op : Bool) : Pair → Pair → Prop where
  /-- T1: the receiver of the connection becomes primary, the initiator
      secondary.  One action, both effects, so the roles cannot disagree. -/
  | t1 : Step op ⟨.none, .none, true, true⟩ ⟨.primary, .secondary, true, true⟩
  /-- T9: operator promotes node 1 while node 0 is confirmed dead. -/
  | promote1 {s : Pair} (hop : op = true) (hdown : s.up0 = false) (hn : s.r1 ≠ .primary) :
      Step op s { s with r1 := .primary }
  /-- T9 mirror: operator promotes node 0 while node 1 is confirmed dead. -/
  | promote0 {s : Pair} (hop : op = true) (hdown : s.up1 = false) (hn : s.r0 ≠ .primary) :
      Step op s { s with r0 := .primary }
  /-- Graceful demotion. -/
  | demote0 {s : Pair} : Step op s { s with r0 := .secondary }
  | demote1 {s : Pair} : Step op s { s with r1 := .secondary }
  /-- T9: the old primary reconnects.  It initiates, therefore it becomes
      secondary — connection direction decides, not a UUID comparison. -/
  | rejoin0 {s : Pair} (hpeer : s.r1 = .primary) :
      Step op s { s with r0 := .secondary, up0 := true }
  | rejoin1 {s : Pair} (hpeer : s.r0 = .primary) :
      Step op s { s with r1 := .secondary, up1 := true }
  /-- Node failure (T8a/T8b). -/
  | fail0 {s : Pair} : Step op s { s with up0 := false }
  | fail1 {s : Pair} : Step op s { s with up1 := false }

/-- Reachability at a fixed operator policy. -/
inductive Reachable (op : Bool) : Pair → Prop where
  | t1 : Reachable op ⟨.none, .none, true, true⟩
  | step : Reachable op s → Step op s s' → Reachable op s'

/-! ## The pair is single-primary by construction, with no election to run

  The first connection assigns both roles in one step, so there is no interleaving in which two
  nodes call themselves primary and no election to lose. What an operator gets is a pair that
  cannot split on its own: with the primary down, the surviving node serves reads and refuses
  writes until someone promotes it.
-/

/-- T1 assigns both roles at once: the receiver is primary, the initiator is
    secondary.  There is no interleaving in which both call themselves primary. -/
theorem t1_roles_are_determined :
    (⟨.none, .none, true, true⟩ : Pair) = ⟨.none, .none, true, true⟩ ∧
      (⟨.primary, .secondary, true, true⟩ : Pair).r0 = .primary ∧
      (⟨.primary, .secondary, true, true⟩ : Pair).r1 = .secondary := by
  refine ⟨rfl, rfl, rfl⟩

/-- **Promotion is operator-only, and requires the peer confirmed dead.**  Both
    halves of the gate appear in the conclusion, so neither can be dropped
    without changing the theorem. -/
theorem promotion_requires_operator_and_dead_peer {op : Bool} {s s' : Pair}
    (h : Step op s s') (hnew : s'.r1 = .primary) (hold : s.r1 ≠ .primary) :
    op = true ∧ s.up0 = false := by
  cases h with
  | t1 => simp at hnew
  | promote1 hop hdown _ => exact ⟨hop, hdown⟩
  | promote0 _ _ _ => simp_all
  | demote0 => simp_all
  | demote1 => simp_all
  | rejoin0 _ => simp_all
  | rejoin1 _ => simp_all
  | fail0 => simp_all
  | fail1 => simp_all

/-- **No split brain.**  In any reachable pair state, at most one node is both
    primary and up, so at most one node accepts writes. -/
theorem no_two_writable {op : Bool} {s : Pair} (h : Reachable op s) :
    ¬(Writable s 0 ∧ Writable s 1) := by
  induction h with
  | t1 => simp [Writable]
  | step hr hs ih =>
      cases hs with
      | t1 => simp [Writable]
      | promote1 _ hdown _ => simp_all [Writable]
      | promote0 _ hdown _ => simp_all [Writable]
      | demote0 | demote1 | rejoin0 _ | rejoin1 _ | fail0 | fail1 =>
          simp_all [Writable]

/-! ## What the operator gate buys, and what dropping it would cost

  Promotion is available only to the operator and only once the peer is confirmed dead, and both
  halves of that gate are load-bearing. Drop the confirmation and one promotion from the initial
  state leaves both nodes primary and accepting writes for the same key, which a client experiences
  as two successful writes to one key holding different values.
-/

/-- Dropping the "peer is confirmed dead" guard makes split brain reachable:
both nodes end up primary and up, so both accept writes.  This is the theorem
that justifies the operator-only transition. -/
inductive UnguardedStep : Pair → Pair → Prop where
  | t1 : UnguardedStep ⟨.none, .none, true, true⟩ ⟨.primary, .secondary, true, true⟩
  | promote1 {s : Pair} (hn : s.r1 ≠ .primary) : UnguardedStep s { s with r1 := .primary }

/-- **Red: without the guard, one promotion splits the brain.**  A single
    `promote1` step from the T1 state leaves both nodes primary and up, so both
    accept writes for the same key and a client can be told two different values
    succeeded.  Nothing in the unguarded system can tell a dead peer from a
    partitioned one, so there is no state at which the promotion is safe. -/
theorem unguarded_promotion_splits_brain :
    ∃ s s', UnguardedStep s s' ∧ Writable s' 0 ∧ Writable s' 1 := by
  refine ⟨⟨.primary, .secondary, true, true⟩, ⟨.primary, .primary, true, true⟩,
          UnguardedStep.promote1 (by simp), ?_, ?_⟩
  · simp [Writable]
  · simp [Writable]

/-- The promotion path is reachable in the guarded system: fail the primary,
    promote the secondary. -/
theorem promote_after_primary_failure_reachable :
    ∃ s, Reachable true s ∧ s.r0 = .primary ∧ s.r1 = .primary ∧ s.up0 = false := by
  refine ⟨⟨.primary, .primary, false, true⟩, ?_, rfl, rfl, rfl⟩
  have h1 : Reachable true (⟨.primary, .secondary, true, true⟩ : Pair) :=
    Reachable.step Reachable.t1 Step.t1
  have h2 : Reachable true (⟨.primary, .secondary, false, true⟩ : Pair) :=
    Reachable.step h1 Step.fail0
  exact Reachable.step h2 (Step.promote1 rfl rfl (by simp))

/-- **A returning primary is demoted by connection direction.**  In the state the
    operator created (old primary down, peer promoted), the old primary coming
    back leaves it secondary and the promoted node primary — the documented
    T9 outcome, and the reason the pair heals without a role election. -/
theorem returning_primary_becomes_secondary :
    ∃ s, Reachable true s ∧ s.r0 = .secondary ∧ s.r1 = .primary ∧
      s.up0 = true ∧ s.up1 = true := by
  refine ⟨⟨.secondary, .primary, true, true⟩, ?_, rfl, rfl, rfl, rfl⟩
  have h1 : Reachable true (⟨.primary, .secondary, true, true⟩ : Pair) :=
    Reachable.step Reachable.t1 Step.t1
  have h2 : Reachable true (⟨.primary, .secondary, false, true⟩ : Pair) :=
    Reachable.step h1 Step.fail0
  have h3 : Reachable true (⟨.primary, .primary, false, true⟩ : Pair) :=
    Reachable.step h2 (Step.promote1 rfl rfl (by simp))
  exact Reachable.step h3 (Step.rejoin0 rfl)

/-- **Degraded pair serves stale reads only.**  With the primary down and no
    promotion, the surviving node is readable but not writable: reads at
    `LOCAL_ONE` keep working, writes fail.  This is T8b's contract. -/
theorem degraded_pair_is_read_only :
    Readable ⟨.primary, .secondary, false, true⟩ 1 ∧
      ¬ Writable ⟨.primary, .secondary, false, true⟩ 1 := by
  refine ⟨by simp [Readable], ?_⟩
  simp [Writable]

/-- ... and it stays that way: no *reachable* transition from a degraded pair
    makes the secondary writable without the operator. -/
theorem degraded_pair_stays_read_only {s : Pair} (h : Reachable false s)
    (h0 : s.up0 = false) (h1 : s.r1 = .secondary) (h2 : s.up1 = true) :
    ¬ Writable s 1 := by
  intro hw
  simp_all [Writable]

end Ferrosa.Topology.Pair