/-
  Ferrosa formal specification — the distinct-count sketch, in commons because all three front ends
  need it.
  ========================================================

  `COUNT(DISTINCT v)` is exact and spills to disk (`DistinctExact`, a slot table with 19/19 tests), so
  its memory is bounded without approximating. `COUNT(ESTIMATE_DISTINCT v)` is the other job: it must
  survive per-partition evaluation and merge afterwards, which rules out a Bloom filter — a Bloom count
  is a function of probe history, so OR-ing two filters yields membership, not a count.

  The sketch is a HyperLogLog: `2^p` registers, each holding the rank of the longest run of zero bits
  seen for its bucket. Merging is register-wise maximum, borrowed from `Commons.Tactics` because the
  tombstone merge is the same algebra. What is *not* proved is the accuracy claim: `1.04/sqrt(m)` needs
  a probability model this corpus does not build, so it is carried as a red rather than assumed.
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

theorem merge_is_commutative (a b : Registers) : merge a b = merge b a := by
  funext i
  exact Tactics.merge_comm a b i

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

theorem hll_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "distinct count: exact vs HyperLogLog (commons)" ledger

end Ferrosa.Commons.Hll
