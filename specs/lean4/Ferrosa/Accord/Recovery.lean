/-
  Ferrosa formal specification — Accord, part 4: recovery, the two ballots, and
  what breaking dependency completeness costs.
  ==========================================================================

  Recovery is where Accord can silently lose an acknowledged commit, and the two ballot
  numbers are what prevent it: the ballot an acceptor promised and the ballot it accepted a
  value at are separate facts, and conflating them lets a Prepare relabel a stale value as
  freshly accepted.  `recovery_diverges` reduces Sutra's counterexample to three acceptors
  and shows the wrong value being chosen — one that no quorum ever accepted, in place of
  one a quorum had committed.  Breaking dependency completeness costs the same way at the
  history level: two transactions that each write one key and read the other's admit no
  serial order, and `dep_guard_restores_serializability` shows the dep-wait guard ruling
  that shape out.
-/
import Ferrosa.Accord.Protocol

namespace Ferrosa.Accord

/-! ## §6.1: the acceptor's two ballot numbers

  An acceptor keeps two ballots: the one it has promised not to undercut, and the one the value it
  holds was accepted at. They are separate facts, and recovery reads the second, proposing the value
  accepted at the highest accepted ballot seen anywhere. Conflating them lets a Prepare relabel a
  stale value as freshly accepted, which is how an acknowledged commit is lost.
-/

/-- `promBallot` is what the acceptor promises not to undercut;
    `accBallot`/`accVal` are what it actually accepted.  These are distinct
    facts and must be stored separately. -/
structure Acceptor where
  promBallot : Nat
  accBallot : Nat
  accVal : Option Val

def Acceptor.init : Acceptor := ⟨0, 0, none⟩

/-- Prepare: raise the promise only. -/
def prepare (a : Acceptor) (b : Nat) : Acceptor :=
  if a.promBallot < b then { a with promBallot := b } else a

/-- Accept: record the value *and* the ballot it was accepted at. -/
def accept (a : Acceptor) (b : Nat) (v : Val) : Acceptor :=
  if a.promBallot ≤ b then { a with accBallot := b, accVal := some v } else a

/-- The conflated version: one ballot field, so a Prepare overwrites the ballot
    the accepted value is recorded under. -/
def prepareConflated (a : Acceptor) (b : Nat) : Acceptor :=
  if a.promBallot < b then { promBallot := b, accBallot := b, accVal := a.accVal } else a

/-- The recovery phase's rule: propose the value accepted at the highest accepted
    ballot seen anywhere. -/
def recoveryChoice (as : List Acceptor) : Option (Nat × Val) :=
  as.foldl (fun best a =>
    match a.accVal with
    | none => best
    | some v =>
        match best with
        | none => some (a.accBallot, v)
        | some (b, _) => if b < a.accBallot then some (a.accBallot, v) else best) none

/-! ### The counterexample, reduced to three acceptors

Three acceptors, one command.  Ballot 3 carries value 2 and is chosen (accepted
by `a1` and `a2`); ballot 1 carries value 1 at `a0` only.  A later Prepare for
ballot 5 reaches `a0`, and with one ballot field that is enough to lose the commit. -/

def a0 : Acceptor := accept (accept (prepare Acceptor.init 1) 1 1) 1 1
def a1 : Acceptor := accept (accept (prepare Acceptor.init 3) 3 2) 3 2
def a2 : Acceptor := accept (accept (prepare Acceptor.init 3) 3 2) 3 2

/-- Correct recovery: ballot 3 with value 2 is the highest accepted, and 2 is the
    value the quorum actually chose. -/
theorem recovery_correct : recoveryChoice [a0, a1, a2] = some (3, 2) := by native_decide

/-- A single Prepare at ballot 5, applied by the conflated acceptor, relabels
    `a0`'s stale value as having been accepted at ballot 5. -/
def a0' : Acceptor := prepareConflated a0 5

/-- **The divergence.**  Recovery now prefers ballot 5 and proposes value 1 —
    which no quorum ever chose, and which contradicts the value 2 that ballot 3
    committed. -/
theorem recovery_diverges : recoveryChoice [a0', a1, a2] = some (5, 1) := by native_decide

/-- **Red: the conflated acceptor proposes a value no quorum ever accepted.**  The same
    three acceptors, after one Prepare at ballot 5, yield `1` where the correct recovery
    yields the committed `2` — so two replicas recovering from this state would agree on
    a value that contradicts the one the client was told was committed. -/
theorem recovery_wrong_value : recoveryChoice [a0', a1, a2] ≠ recoveryChoice [a0, a1, a2] := by
  rw [recovery_diverges, recovery_correct]
  simp

/-- **The precise defect**: a Prepare moved the *accepted* ballot from 1 to 5
    while the value stayed put.  No Accept ever ran at ballot 5, so the pair
    `(5, some 1)` is a claim about a ballot at which nothing was accepted —
    which is precisely what recovery then trusts. -/
theorem conflation_relabels_without_accept :
    a0'.accBallot = 5 ∧ a0.accBallot = 1 ∧ a0'.accVal = a0.accVal := by
  refine ⟨rfl, rfl, rfl⟩

/-- In the correct acceptor, an accepted value is always recorded at a ballot the
    acceptor had already promised — the relation §6.1 relies on. -/
theorem correct_ballot_invariant (a : Acceptor) (b : Nat) (v : Val)
    (h : a.promBallot ≤ b) : (accept a b v).promBallot ≤ (accept a b v).accBallot := by
  unfold accept
  rw [if_pos h]
  exact h

/-! ## What a dropped dependency costs

The two transactions below touch both keys in opposite orders, each reading the
other's key before the other wrote it.  Both observations are individually
plausible; together they admit no serial order. -/

/-- A: reads k1 (sees `[]`), appends 10 to k0.  B: reads k0 (sees `[]`), appends
    20 to k1.  Neither saw the other's write. -/
def hWriteSkew : History := ⟨[
  invEv 0 1 [.read 1, .append 0 10] 0,
  okEv 0 1 [.read 1, .append 0 10] [[]] 0 10,
  invEv 1 2 [.read 0, .append 1 20] 10,
  okEv 1 2 [.read 0, .append 1 20] [[]] 10 20]⟩

/-- **Not serializable**: A must precede B (B did not see A's write to k0) and B
    must precede A (A did not see B's write to k1). -/
theorem write_skew_not_serializable : serializableB hWriteSkew = false := by native_decide

/-- The same pair, with B's dependency on A intact: B now observes A's append.
    This history *is* strictly serializable. -/
def hSkewGuarded : History := ⟨[
  invEv 0 1 [.read 1, .append 0 10] 0,
  okEv 0 1 [.read 1, .append 0 10] [[]] 0 10,
  invEv 1 2 [.read 0, .append 1 20] 20,
  okEv 1 2 [.read 0, .append 1 20] [[10]] 20 30]⟩

/-- With the dependency intact the write-skew pair is strictly serializable — B's read
    observes A's append — so it is the missing edge, not the checker, that made the
    unguarded history fail. -/
theorem dep_guard_restores_serializability : strictlySerializableB hSkewGuarded = true := by
  native_decide

/-- And the unguarded history is rejected by the strict checker too — dropping a
    dependency is not a licence, it is a defect. -/
theorem write_skew_not_strict : strictlySerializableB hWriteSkew = false := by native_decide

end Ferrosa.Accord
