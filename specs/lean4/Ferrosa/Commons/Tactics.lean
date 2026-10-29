/-
  Ferrosa formal specification — shared tactics and lemma library.
  ================================================================

  Everything here is used by more than one specification module, and each lemma names the places that
  use it so a future deletion is caught. The point of this file is that the corpus stops re-deriving
  the join-semilattice laws: the tombstone merge and the distinct-count sketch are the same algebra on
  different carriers, and so are the buffer accounting bounds used by SUBSCRIBE and the streaming path.
-/
import Ferrosa.Core

namespace Ferrosa.Commons.Tactics

/-! ### Merge laws — used by `Storage.Tombstone` and `Commons.Hll`

Both merges are register-wise / cell-wise `max`: the tombstone wins by timestamp, the sketch register
keeps the longest zero run. Associative, commutative, idempotent is what buys order independence and
retry safety in both places, so it is proved once. -/

variable {α : Type}

theorem merge_comm (a b : Nat → Nat) (i : Nat) : max (a i) (b i) = max (b i) (a i) := by
  omega

theorem merge_assoc (a b c : Nat → Nat) (i : Nat) :
    max (max (a i) (b i)) (c i) = max (a i) (max (b i) (c i)) := by
  omega

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

theorem min_add_sub (a c : Nat) : min a c + (a - c) = a := by
  omega

theorem min_le_left (a b : Nat) : min a b ≤ a := by omega

theorem min_le_right (a b : Nat) : min a b ≤ b := by omega

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
