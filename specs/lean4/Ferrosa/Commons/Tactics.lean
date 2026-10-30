/-
  Shared merge laws and the accounting identity behind every buffer bound
  ======================================================================

  Two algebras and one arithmetic fact carry most of the corpus's reasoning about order and memory.
  Pointwise maximum is associative, commutative and idempotent, and that single fact is what lets
  `Storage.Tombstone` resolve concurrent writes in any order and `Commons.Hll` merge partial
  sketches without a canonical fold; the identity `min a c + (a - c) = a` is what every conservation
  statement about a buffer — resident plus spilled equals what was pushed — reduces to, in
  `Cdc.Subscribe` and `Storage.Streaming` alike.

  The lemmas are here so that neither argument is re-derived per surface; each names the modules
  that lean on it, which is how a deletion stays visible rather than silent.
-/
import Ferrosa.Core

namespace Ferrosa.Commons.Tactics

/-! ### Merge laws — used by `Storage.Tombstone` and `Commons.Hll`

Both merges are register-wise / cell-wise `max`: the tombstone wins by timestamp, the sketch register
keeps the longest zero run. Associative, commutative, idempotent is what buys order independence and
retry safety in both places, so it is proved once. -/

variable {α : Type}

/-- Pointwise maximum does not care which argument comes first — the law `Storage.Tombstone` uses to
    converge two replicas whatever order they merge in, and `Commons.Hll` to fold partial sketches. -/
theorem merge_comm (a b : Nat → Nat) (i : Nat) : max (a i) (b i) = max (b i) (a i) := by
  omega

/-- Grouping is irrelevant to pointwise maximum, so a fan-in of merges needs no canonical tree shape. -/
theorem merge_assoc (a b c : Nat → Nat) (i : Nat) :
    max (max (a i) (b i)) (c i) = max (a i) (max (b i) (c i)) := by
  omega

/-- Merging a value with itself returns it, which is what makes a retried merge harmless: it cannot
    change a tombstone's choice or inflate a sketch register. -/
theorem merge_idem (a : Nat → Nat) (i : Nat) : max (a i) (a i) = a i := by
  omega

/-- Two pointwise-max merges agree if they agree register by register. -/
theorem merge_ext {a b c d : Nat → Nat}
    (h : ∀ i, max (a i) (b i) = max (c i) (d i)) :
    (fun i => max (a i) (b i)) = fun i => max (c i) (d i) := by
  funext i
  exact h i

/-! ### Accounting bounds — used by `Cdc.Subscribe` and `Storage.Streaming`

Resident bytes are `min backlog share`, and every conservation statement about a buffer reduces to
`min a c + (a - c) = a`. -/

/-- Resident plus spilled equals the backlog: what a buffer still holds and what it moved to disk
    together account for everything pushed, which is the identity the spill bounds in
    `Cdc.Subscribe` and `Storage.Streaming` are stated with. -/
theorem min_add_sub (a c : Nat) : min a c + (a - c) = a := by
  omega

/-- A minimum is no greater than its first argument: a buffer never holds more than either the
    backlog or its share. -/
theorem min_le_left (a b : Nat) : min a b ≤ a := by omega

/-- A minimum is no greater than its second argument, so resident bytes stay inside the allocation
    as well as inside the backlog. -/
theorem min_le_right (a b : Nat) : min a b ≤ b := by omega

/-- The bound is the hypothesis here: the statement assumes the share already fits and concludes it,
    so it names the obligation a share calculation owes rather than establishing it for any
    particular scheduler. -/
theorem a_bounded_share_stays_inside_its_bound (a bound : Nat) (h : min a bound ≤ bound) :
    min a bound ≤ bound := h

/-- A consumer holding at most its share cannot exceed the pool if the share cannot. -/
theorem a_share_inside_the_pool_stays_inside (resident share pool : Nat)
    (h1 : resident ≤ share) (h2 : share ≤ pool) : resident ≤ pool :=
  Nat.le_trans h1 h2

/-! ### Finite-configuration checks

`decide` is for closed arithmetic; `native_decide` is for finite computations over concrete data (a
list of configurations, a ledger count). Neither is used for anything quantified over an unbounded
domain — those use `omega` or explicit induction. -/

end Ferrosa.Commons.Tactics
