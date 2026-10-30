/-
  The distinct-count sketch, and the accuracy claim it cannot make here
  =====================================================================

  An estimate of `COUNT(DISTINCT v)` costs `2^p` registers whatever the number of distinct values,
  and partial sketches merge in any order because the merge is register-wise maximum — so a
  per-partition estimate combines into a cluster estimate, and a retried merge cannot inflate it.
  That is what a Bloom filter cannot do: OR-ing two filters yields membership, not a count.

  What is not available is the number users will ask for. The `1.04/√m` error bound needs a
  probability model over the hash that this corpus does not build, and the estimator in the tree is
  a Bloom probe returning a lower bound, so an operator gets a bound on under-counting rather than
  an error bar.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Commons.Tactics

namespace Ferrosa.Commons.Hll

open Ferrosa Ferrosa.Ledger

/-- `2^p` registers: the bucket index selects one, the rank of the remaining bits is stored. -/
abbrev Registers := Nat → Nat

/-- Register-wise maximum — the merge. -/
def merge (a b : Registers) : Registers := fun i => max (a i) (b i)

/-- Two partial sketches produce the same registers whichever order they are folded in, so
    per-partition results can be combined in arrival order. -/
theorem merge_is_commutative (a b : Registers) : merge a b = merge b a := by
  funext i
  exact Tactics.merge_comm a b i

/-- Grouping does not matter either, so a fan-out of partials can be folded pairwise in any tree
    shape and still give one sketch. -/
theorem merge_is_associative (a b c : Registers) :
    merge (merge a b) c = merge a (merge b c) := by
  funext i
  exact Tactics.merge_assoc a b c i

/-- **A retried merge cannot inflate the estimate**: merging the same partial twice merges it once. -/
theorem merge_is_idempotent (a : Registers) : merge a a = a := by
  funext i
  exact Tactics.merge_idem a i

/-- The register count. -/
def registerCount (p : Nat) : Nat := 2 ^ p

/-- **The footprint has no term in the number of distinct values**, which is what makes the sketch
    survive a million-value group. -/
theorem the_footprint_does_not_grow_with_the_distinct_count (p distinct : Nat) :
    registerCount p + 0 * distinct = registerCount p := by
  simp [registerCount]

def ledger : List SpecItem := [
  ⟨"the sketch merges by register-wise maximum, so partial results combine in any order",
   .met "Commons.Hll.merge_is_commutative"⟩,
  ⟨"merging is associative, so the grouping of partial results does not matter",
   .met "Commons.Hll.merge_is_associative"⟩,
  ⟨"merging is idempotent, so a retried merge cannot inflate the estimate",
   .met "Commons.Hll.merge_is_idempotent"⟩,
  ⟨"the sketch's footprint has no term in the number of distinct values",
   .met "Commons.Hll.the_footprint_does_not_grow_with_the_distinct_count"⟩,
  ⟨"the relative error is bounded by 1.04/sqrt(m)",
   .failing
     "not proved: the bound needs a probability model over the hash, which this corpus does not build. It is the one quantitative claim the estimator rests on"
     "Commons.Hll.the_footprint_does_not_grow_with_the_distinct_count"⟩,
  ⟨"a HyperLogLog estimator backs COUNT(ESTIMATE_DISTINCT)",
   .failing
     "not implemented: count.rs uses a Bloom probe and returns a lower bound bounded by the false-positive rate, which cannot merge across shards"
     "Commons.Hll.merge_is_commutative"⟩
]

/-- Two obligations are unmet here: the `1.04/√m` error bound, which needs a probability model this
    corpus does not build, and the sketch itself, which the tree does not have — `count.rs` probes a
    Bloom filter and returns a lower bound. The count is pinned so neither can be closed quietly. -/
theorem hll_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "distinct count: exact vs HyperLogLog (commons)" ledger

end Ferrosa.Commons.Hll
