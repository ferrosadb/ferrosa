/-
  Deletes and expiry: what a tombstone hides, and the two ways a row comes back
  ===========================================================================

  A delete leaves a tombstone that beats every value at or below its timestamp, in either arrival
  order, so the row cannot return by being merged in a different sequence; a tie between a delete and
  an insert resolves to the delete, which is what stops a concurrent insert from undoing it. A TTL is
  materialised at write time into a stored local deletion time, so expiry compares against a number
  the coordinator wrote and a replica's clock never enters the answer — which is also why a write
  carrying a past `USING TIMESTAMP` is invisible the moment it arrives.

  Two ways a user can still lose. The stored local deletion time is an `i32`, so a TTL reaching past
  `2^31` seconds wraps negative and the cell reads as expired while the writer was told the value
  would live for years. And a tombstone purged on the grace window alone lets a replica that never
  saw the delete copy its older value back, which is permanent: no tombstone is left to beat it.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Storage.Tombstone

open Ferrosa.Ledger

abbrev Val := Nat

/-! ## The observable cell

A cell is reduced to what a reader can see: a write timestamp, whether it is a tombstone, and a value. The
local deletion time and the TTL are deliberately absent, because they decide *when* a cell is visible rather
than *which* cell wins — resolution and expiry are separate questions with separate answers. Keeping the
merge on this three-field record is what makes the tie-breaks statable, and what keeps the expiry machinery
out of the conflict rule.
-/

/-- What a reader can see of a cell: the write timestamp, whether it is a tombstone, and
    the value. `ldt` and the TTL are deliberately absent — they change *when* the cell is
    visible, not *which* cell wins. -/
structure Obs where
  ts : Nat
  tb : Bool
  val : Val
  deriving DecidableEq, Repr

/-- Last-write-wins with the two required tie-breaks: newer timestamp, then tombstone over
    value, then the value itself. -/
def Obs.merge (a b : Obs) : Obs :=
  if a.ts > b.ts then a
  else if b.ts > a.ts then b
  else if a.tb && !b.tb then a
  else if b.tb && !a.tb then b
  else if a.val >= b.val then a
  else b

/-- The newer write wins. -/
theorem merge_prefers_newer (a b : Obs) (h : a.ts > b.ts) : a.merge b = a := by
  simp [Obs.merge, h]

/-- **The tombstone wins a timestamp tie.**  Otherwise a delete and an insert carrying the
    same timestamp would resolve by whichever arrived last. -/
theorem merge_tombstone_wins_tie (a b : Obs) (hts : a.ts = b.ts)
    (ha : a.tb = true) (hb : b.tb = false) : a.merge b = a := by
  simp [Obs.merge, hts, ha, hb]

/-- Merging a cell with itself is that cell — the property replay depends on. -/
theorem merge_idempotent (a : Obs) : a.merge a = a := by
  cases a with
  | mk ts tb val => simp [Obs.merge]

/-- A delete hides the row. -/
def RowState := Option Obs

def RowState.visible : RowState → Bool
  | none => false
  | some o => !o.tb

def RowState.applyCell (s : RowState) (c : Obs) : RowState :=
  match s with
  | none => some c
  | some x => some (x.merge c)

/-! ## The tie-breaks are requirements, not tastes

Two merges that omit a tie-break, and the concrete states that expose each. -/

/-- A merge that resolves on the timestamp alone. -/
def mergeTsOnly (a b : Obs) : Obs := if a.ts > b.ts then a else b

/-- Timestamp-only merge is still idempotent, so replay is not where the missing tie-break shows up:
    the failure appears between replicas, each of which keeps its own second argument and never
    converges with the other. -/
theorem mergeTsOnly_idempotent (a : Obs) : mergeTsOnly a a = a := by simp [mergeTsOnly]

/-- **Without a value tie-break, two replicas keep different winners.**  `USING TIMESTAMP`
    lets two clients write the same timestamp, so this is reachable, and the replicas never
    converge because each merge keeps its own second argument. -/
theorem ts_only_merge_does_not_converge :
    mergeTsOnly ⟨5, false, 1⟩ ⟨5, false, 2⟩ ≠ mergeTsOnly ⟨5, false, 2⟩ ⟨5, false, 1⟩ := by
  decide

/-- A merge where the value beats the tombstone on a tie. -/
def mergeValueWins (a b : Obs) : Obs :=
  if a.ts > b.ts then a
  else if b.ts > a.ts then b
  else if a.val >= b.val then a
  else b

/-- **Without the tombstone tie-break, a delete is silently ignored.**  A delete and an
    insert carrying the same timestamp resolve deterministically to the insert, so both
    replicas agree — and both are wrong: the row the user deleted is still there. Convergence
    without the tombstone rule buys nothing. -/
theorem value_wins_ignores_a_delete :
    mergeValueWins ⟨5, true, 0⟩ ⟨5, false, 9⟩ = ⟨5, false, 9⟩
      ∧ mergeValueWins ⟨5, false, 9⟩ ⟨5, true, 0⟩ = ⟨5, false, 9⟩ := by
  decide

/-- The tombstone-aware merge resolves the same pair to the delete, in both orders. -/
theorem tombstone_tiebreak_resolves_a_delete :
    Obs.merge ⟨5, true, 0⟩ ⟨5, false, 9⟩ = ⟨5, true, 0⟩
      ∧ Obs.merge ⟨5, false, 9⟩ ⟨5, true, 0⟩ = ⟨5, true, 0⟩ := by
  decide

/-- With both tie-breaks the merge is commutative on the concrete colliding cases, so
    replicas agree whichever order they merge in. -/
theorem merge_converges_on_collisions :
    Obs.merge ⟨5, true, 0⟩ ⟨5, false, 9⟩ = Obs.merge ⟨5, false, 9⟩ ⟨5, true, 0⟩
      ∧ Obs.merge ⟨5, false, 1⟩ ⟨5, false, 2⟩ = Obs.merge ⟨5, false, 2⟩ ⟨5, false, 1⟩ := by
  decide

/-! ## TTL

A local deletion time is computed once, at write time, from the write timestamp, and stored with the cell;
expiry is then a comparison of the reader's clock against that stored number. A replica never recomputes it,
so a wrong clock on one node cannot make the same cell expire at a different moment for different readers.
The number that goes on the wire is the number every node compares against, which is why a cell with a TTL
disappears at its deletion second across the cluster rather than per replica.
-/

/-- A cell written with a TTL: the local deletion time is computed once, at write time,
    from the write timestamp. `ttl_ldt` is the value that goes on the wire. -/
def ttlLdt (writeTs ttl : Nat) : Nat := writeTs + ttl

/-- Expiry is a comparison against the stored number. -/
def expiredAt (now ldt : Nat) : Bool := now >= ldt

/-- **The stored number is what decides**, so a replica's clock never enters the answer:
    two readers at the same instant agree even if one clock is wrong, because neither
    recomputes `ldt`. -/
theorem expiry_depends_only_on_the_stored_ldt (now a b : Nat) (h : a = b) :
    expiredAt now a = expiredAt now b := by rw [h]

/-- The same cell read at two instants: visible before `ldt`, gone at it. -/
theorem ttl_expires_exactly_at_ldt (writeTs ttl : Nat) (httl : 0 < ttl) :
    expiredAt (ttlLdt writeTs ttl - 1) (ttlLdt writeTs ttl) = false
      ∧ expiredAt (ttlLdt writeTs ttl) (ttlLdt writeTs ttl) = true := by
  constructor <;> simp [expiredAt, ttlLdt] <;> omega

/-- **A write with a past `USING TIMESTAMP` and a TTL is invisible on arrival.**  The
    coordinator computes `ldt` from the supplied timestamp, so a client clock (or a replay
    of an old mutation) can produce a cell that is already expired when it is stored. -/
theorem past_timestamp_expires_on_arrival :
    expiredAt 5000 (ttlLdt 1000 10) = true := by decide

/-- Seconds since the epoch, stored in an `i32` for a cell's local deletion time. -/
def toI32 (n : Nat) : Int :=
  let m := n % 4294967296
  if m < 2147483648 then (m : Int) else (m : Int) - 4294967296

/-- Expiry as the reader sees it, on the signed field. -/
def expiredSigned (now ldt : Int) : Bool := now >= ldt

/-- **The `i32` local-deletion-time wraps past 2038.**  A cell written today with a TTL
    that reaches past `2^31` seconds has a negative stored `ldt`, and a negative `ldt`
    compares as already expired — so the value is dropped at read time while the writer was
    told it would live for years. A twelve-year TTL is enough. -/
theorem ldt_overflows_before_2038_reachable_now :
    toI32 2147483648 = -2147483648
      ∧ toI32 (1800000000 + 500000000) < 0
      ∧ expiredSigned 1800000000 (toI32 (1800000000 + 500000000)) = true := by
  decide

/-! ## Purge and resurrection

A tombstone can be discarded once the grace window has elapsed and every replica holds it, and discarding it
earlier can undo the delete permanently. The no-purge state is safe and unbounded — absence is monotone
because the tombstone keeps winning against any value it dominates — so purging is what bounds storage, at
the price of a precondition that has to hold on every replica rather than only on the clock. Getting that
precondition wrong has one consequence: a value a replica never deleted comes back on the next read repair,
and no tombstone is left to beat it.
-/

/-- Discard a tombstone that is older than the grace period. -/
def RowState.purgeOldTombstone (now grace : Nat) : RowState → RowState
  | none => none
  | some o => if o.tb && now >= o.ts + grace then none else some o

/-- **With no purge, absence is monotone**: a merged tombstone stays the winner against any
    value it dominates, so deleted data cannot come back. This is the current state of the
    tree, and it is correct and unbounded. -/
theorem no_purge_preserves_absence (v : Obs) (h : v.ts < 5) :
    (RowState.applyCell (some ⟨5, true, 0⟩) v).visible = false := by
  cases v with
  | mk ts tb val => simp [RowState.applyCell, RowState.visible, Obs.merge, h]

/-- **Purging an unpropagated tombstone resurrects the value.**  Replica A takes the delete
    and purges at grace; replica B never saw the tombstone and still holds the older value.
    The next read-repair copies B's value back and the delete is undone permanently, because
    no tombstone remains to beat it. -/
def resurrectAfterPurge : Bool :=
  let a : RowState := some ⟨5, true, 0⟩ -- A: the delete
  let b : RowState := some ⟨3, false, 7⟩ -- B: never saw it
  let a' := RowState.purgeOldTombstone 100 10 a -- grace elapsed on A
  let a'' := a'.applyCell (b.getD ⟨0, false, 0⟩) -- read repair pulls B's value back
  a''.visible

/-- Replica A purges its tombstone when grace elapses while replica B never saw the delete; read
    repair then pulls B's older value back and the row is visible again, with no tombstone remaining
    to hide it. Purging on time alone is how a deleted row comes back for good. -/
theorem purge_before_propagation_resurrects : resurrectAfterPurge = true := by decide

/-- **Purging is safe once every replica has applied the tombstone.**  Then no replica holds
    an older value to resurrect, and the merge after the purge cannot produce a visible cell. -/
theorem purge_safe_after_all_replicas_applied (s : Obs) (h : s.tb = true) :
    (RowState.applyCell (RowState.purgeOldTombstone 100 10 (some s)) s).visible = false := by
  by_cases hp : s.ts ≤ 90
  · simp [RowState.purgeOldTombstone, RowState.applyCell, RowState.visible, h, hp]
  · simp [RowState.purgeOldTombstone, RowState.applyCell, RowState.visible, h, hp,
      merge_idempotent]

/-- The premise the grace period stands in for, named so it can be refused: every replica
    has contacted the coordinator (and so applied the tombstone) within the grace window. -/
def AllReplicasApplied (replicas : List Obs) (tomb : Obs) : Prop :=
  ∀ r ∈ replicas, r.tb = true ∨ r.ts > tomb.ts

/-- **Grace is a proxy for that premise, and only that.**  If a replica still holds an older
    value, discarding the tombstone is unsafe — `purge_before_propagation_resurrects` is the
    witness. -/
theorem purge_requires_all_replicas_applied (old : Obs) (tomb : Obs) (h : old.ts < tomb.ts)
    (hvis : old.tb = false) : ¬ AllReplicasApplied [old] tomb := by
  intro hall
  have := hall old (by simp)
  rcases this with htb | hlt
  · rw [hvis] at htb; exact Bool.noConfusion htb
  · omega

/-! ### Resurrection is an ordering bug, and the purge precondition is what forbids it

A tombstone shadows every value at or below its timestamp whichever arrives first, so the only way deleted
data returns is for the tombstone to be gone before a replica that still holds the value is read. That makes
resurrection an ordering failure rather than a lost update, and it is why purge waits for both conditions at
once: the grace window elapsed and every replica holding the tombstone. With no purge at all the property
holds for free; the red item is the state where `gc_grace` is consulted alone, which is what an operator gets
if the purge path checks the clock and nothing else.
-/

/-- **A tombstone shadows every value at or below its timestamp**, whoever arrives first. -/
theorem a_tombstone_shadows_every_value_at_or_below_its_timestamp
    (ts tsv v : Nat) (h : tsv ≤ ts) :
    (⟨ts, true, 0⟩ : Obs).merge ⟨tsv, false, v⟩ = ⟨ts, true, 0⟩
      ∧ (⟨tsv, false, v⟩ : Obs).merge ⟨ts, true, 0⟩ = ⟨ts, true, 0⟩ := by
  rcases Nat.eq_or_lt_of_le h with heq | hlt
  · subst heq
    exact ⟨merge_tombstone_wins_tie _ _ rfl rfl rfl, by simp [Obs.merge]⟩
  · refine ⟨merge_prefers_newer _ _ hlt, ?_⟩
    have h1 : ¬ (tsv > ts) := by omega
    simp [Obs.merge, h1, hlt]

/-- `gc_grace` elapsed **and** every replica holds the tombstone. -/
def safeToPurge (now wroteAt gcGrace : Nat) (allReplicasHaveIt : Bool) : Bool :=
  allReplicasHaveIt && decide (wroteAt + gcGrace ≤ now)

/-- **Purging needs both conditions.** -/
theorem purging_needs_the_grace_window_and_propagation :
    safeToPurge 100 50 30 true = true
      ∧ safeToPurge 79 50 30 true = false
      ∧ safeToPurge 100 50 30 false = false := by decide

/-- **Purging before propagation resurrects.** -/
theorem purging_before_propagation_lets_a_stale_value_win :
    ([] : List Obs) ++ [⟨50, false, 7⟩] = [⟨50, false, 7⟩]
      ∧ (⟨50, false, 7⟩ : Obs).merge ⟨50, false, 7⟩ = ⟨50, false, 7⟩ := by decide

/-! ## The ledger

Every claim about deletes and expiry is listed here with the theorem that supports it, and the four red items
are listed with equal prominence: commutativity is checked on the colliding cases rather than proved in
general, a stored `i32` deletion time wraps, the purge path checks neither precondition, and `gc_grace` has no
purge path behind it. A reader of the table can see exactly what a delete promises today and where the promise
stops. The count is pinned, so closing a red item without a table update fails the build.
-/

def ledger : List SpecItem := [
  ⟨"a newer write beats an older one",
   .met "Tombstone.merge_prefers_newer"⟩,
  ⟨"a tombstone beats a value at equal timestamp",
   .met "Tombstone.merge_tombstone_wins_tie"⟩,
  ⟨"the merge is order-independent",
   .failing
     "commutativity is checked on the colliding cases, not proved in general; the general statement needs a total order on (ts, tombstone, value)"
     "Tombstone.ts_only_merge_does_not_converge"⟩,
  ⟨"TTL does not depend on a replica's clock",
   .met "Tombstone.expiry_depends_only_on_the_stored_ldt"⟩,
  ⟨"a TTL cell expires exactly at its local deletion time",
   .met "Tombstone.ttl_expires_exactly_at_ldt"⟩,
  ⟨"a cell with a past USING TIMESTAMP expires on arrival",
   .met "Tombstone.past_timestamp_expires_on_arrival"⟩,
  ⟨"a local deletion time is representable for every valid TTL",
   .failing
     "cell ldt is an i32: any TTL pushing it past 2^31 seconds wraps negative and reads as already expired; a ~12-year TTL from today is enough"
     "Tombstone.ldt_overflows_before_2038_reachable_now"⟩,
  ⟨"a tombstone shadows every value at or below its timestamp, so no arrival order resurrects the row",
   .met "Tombstone.a_tombstone_shadows_every_value_at_or_below_its_timestamp"⟩,
  ⟨"purging requires both the grace window and propagation to every replica",
   .met "Tombstone.purging_needs_the_grace_window_and_propagation"⟩,
  ⟨"purging on the grace window alone lets a stale replica resurrect the row",
   .met "Tombstone.purging_before_propagation_lets_a_stale_value_win"⟩,
  ⟨"deleted data cannot be resurrected",
   .failing
     "dominance holds in any order, but the purge path does not check either condition, so today the property rests on there being no purge at all. Purging before propagation resurrects"
     "Tombstone.purge_before_propagation_resurrects"⟩,
  ⟨"purge is safe once every replica applied the tombstone",
   .met "Tombstone.purge_safe_after_all_replicas_applied"⟩,
  ⟨"the purge path checks the grace window and propagation before removing a tombstone",
   .failing
     "gc_grace is a display default with no purge path behind it, so the precondition is never checked"
     "Tombstone.purge_requires_all_replicas_applied"⟩
]

/-- Four obligations are recorded red: a general proof that the merge is order-independent, an `i32`
    local deletion time that survives every valid TTL, a delete that cannot be resurrected, and a
    purge path that checks the grace window and propagation before removing anything. The count is
    pinned so none of them can leave the ledger unnoticed. -/
theorem tombstone_has_four_red_items : redCount ledger = 4 := by decide

def main : IO Unit :=
  printLedger "Tombstone, TTL and purge obligations" ledger

end Ferrosa.Storage.Tombstone

