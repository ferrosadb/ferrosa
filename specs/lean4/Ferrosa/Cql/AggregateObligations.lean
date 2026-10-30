/-
  Ferrosa formal specification — CQL aggregates, part 9: what is still owed.
  =====================================================================

  Six of the obligations attached to the aggregate functions are not met, and each
  is written as a theorem exhibiting why rather than as a note saying so. Two are
  gaps in the implementation; the other four are things the model here cannot
  state at all, which is the more interesting half — `COUNT(DISTINCT)` keeps its
  visited set in memory, so the constant-memory obligation is met by the Rust
  `DistinctExact` and not by the model, and the refinement between them is
  unproved.

  Writing the failures as theorems is what keeps them honest. When the
  implementation changes, the witness stops holding, the build goes red, and the
  item has to move to `met` with a real proof — the loop closed by the compiler
  rather than by a comment. `Ferrosa/Cql/Aggregate.lean` proves the contracts these
  items are measured against.
-/
import Ferrosa.Cql.Aggregate

set_option linter.deprecated false

namespace Ferrosa.Cql.AggregateObligations

open Ferrosa
open Ferrosa.Cql.Aggregate

abbrev Val := Nat

/-! ## The ledger -/

/-- An item is either discharged by a named theorem or red for a stated reason. -/
inductive Status where
  /-- Discharged; the string names the theorem that proves it. -/
  | met (proof : String)
  /-- Not discharged; the string says what is missing, and `witness` names the theorem
      exhibiting the gap where one exists. -/
  | failing (why : String) (witness : String)
  deriving Repr

structure SpecItem where
  name : String
  status : Status
  deriving Repr

def render (i : SpecItem) : String :=
  match i.status with
  | .met proof => s!"  PASS  {i.name}\n          proved by {proof}"
  | .failing why wit => s!"  RED   {i.name}\n          why: {why}\n          witness: {wit}"

/-! ## The items that are met, with their discharging theorem -/

/-- `COUNT(*)` over the rows is their number, so a row count never has to be
    estimated or approximated. -/
theorem count_star_is_exact (rows : List Val) : foldCount rows 0 = rows.length := by
  rw [foldCount_eq]; omega

/-- Two row sets of the same length produce the same `COUNT(*)`, so the answer
    carries nothing about any individual row. -/
theorem count_star_retains_no_row (a b : List Val) (h : a.length = b.length) :
    foldCount a 0 = foldCount b 0 :=
  count_forgets_which_rows a b h

/-! ## Red item 1 — the model of exact distinct is not O(1) in memory

The Rust `DistinctExact` streams values to a disk-backed indexed map, so its in-memory
footprint is constant. The *model* threads the visited set as a `List`, so it holds one
entry per distinct value. That is not a bug in the model — a model may abstract the
storage — but it does mean the O(1)-memory obligation is **not** discharged here, and
the refinement from `walkExactPair` to `DistinctExact` is not proved. -/

/-- The model holds one entry per distinct value, so the count and the memory the
    visited set occupies are the same number. That is why the constant-memory
    obligation is red here even though the implementation's `DistinctExact` keeps
    its map on disk. -/
theorem model_visited_set_length_is_the_count (rows : List Val) :
    (walkExactPair rows (0, [])).2.length = (walkExactPair rows (0, [])).1 :=
  (exact_distinct_is_cardinality rows).symm

/-! ## Red item 2 — `COUNT(DISTINCT)` does not decompose across shards -/

/-- The distinct count of one shard. -/
def shardDistinct (rows : List Val) : Nat := (walkExactPair rows (0, [])).1

/-- The distinct count of the union of two shards' rows. -/
def globalDistinct (a b : List Val) : Nat := (walkExactPair (a ++ b) (0, [])).1

/-- **Summing per-shard counts over-counts.**  One value present in two shards is one
    distinct value globally and two counted ones. A distributed `COUNT(DISTINCT)` must
    merge the per-shard sets (or sketches), never the counts. -/
theorem sum_of_shard_counts_overcounts :
    shardDistinct [0] + shardDistinct [0] = 2 ∧ globalDistinct [0] [0] = 1 := by
  decide

/-- The same with several values, so the gap is not an artefact of a single row. -/
theorem shard_sum_overcounts_by_the_overlap :
    shardDistinct [0, 1] + shardDistinct [1, 2] = 4 ∧ globalDistinct [0, 1] [1, 2] = 3 := by
  decide

/-! ## Red item 3 — a Bloom filter cannot be merged into a count

Two shards' filters can be OR-ed into a filter, but the merged *count* is not recoverable
from the bits: two different row sets produce the same bit array with different counts.
So `COUNT(ESTIMATE_DISTINCT)` across shards needs a mergeable sketch (HLL) or a re-scan —
not a sum of per-shard estimates, and not an OR of filters. -/

/-- The bit array of a set of rows, as a list so it can be compared and inspected. -/
def bloomBits {m : Nat} (h : Val → List (Fin m)) (rows : List Val) : List Bool :=
  (List.finRange m).map
    (rows.foldl (fun (b : Bloom m) (v : Val) => Bloom.insert b (h v)) Bloom.empty)

/-- The estimate a set of rows produces. -/
def bloomSeen {m : Nat} (h : Val → List (Fin m)) (rows : List Val) : Nat :=
  (walkFrom h rows 0 [] 0 Bloom.empty).2

/-- `0` probes both bits, `1` only the first, `2` only the second. -/
def twoBitHash : Val → List (Fin 2) :=
  fun v => if v = 0 then [0, 1] else if v = 1 then [0] else [1]

/-- **The bits do not determine the count.**  `[0]` and `[1, 2]` leave identical bit
    arrays — `{0, 1}` — while counting one and two distinct values. A merge that keeps
    only bits therefore cannot produce the merged count. -/
theorem bloom_bits_do_not_determine_the_count :
    bloomBits twoBitHash [0] = bloomBits twoBitHash [1, 2]
      ∧ bloomSeen twoBitHash [0] = 1
      ∧ bloomSeen twoBitHash [1, 2] = 2 := by
  decide

/-- And summing per-shard estimates over-counts on the overlap, exactly as the exact
    counts do. -/
theorem sum_of_estimates_overcounts :
    bloomSeen twoBitHash [0] + bloomSeen twoBitHash [0] = 2
      ∧ bloomSeen twoBitHash [0, 0] = 1 := by
  decide

/-! ## Red item 4 — the estimate's error bound is unproved

`estimate_le_exact` gives monotonicity in one direction. The quantitative statement a
user needs is `estimate ≥ (1 - fpr) * exact`, and nothing here establishes it: the
bound depends on the filter's fill, which `DistinctEstimate::estimated_false_positive_rate`
reports at runtime but the model does not constrain. `estimate_can_be_strict` shows no
exactness claim can stand in for it. -/

/-- The obligation, stated so it can be discharged later: the estimate is within a
    relative error `ε` of the exact count whenever the filter's fill stays below `f`. -/
def RelativeErrorBound (h : Val → List (Fin 8)) (ε : Nat) : Prop :=
  ∀ rows : List Val,
    (walkFrom h rows 0 [] 0 Bloom.empty).1 ≤ (walkFrom h rows 0 [] 0 Bloom.empty).2 + ε

/-- The obligation is **not** satisfied for `ε = 0`: a colliding filter under-counts. -/
theorem exactness_does_not_hold :
    ¬ RelativeErrorBound collidingHash 0 := by
  intro h
  have := h [0, 1]
  have hlt := estimate_can_be_strict
  omega

/-! ## The ledger -/

def ledger : List SpecItem := [
  ⟨"COUNT(*) is exact",
   .met "AggregateObligations.count_star_is_exact"⟩,
  ⟨"COUNT(*) retains no row",
   .met "AggregateObligations.count_star_retains_no_row"⟩,
  ⟨"COUNT(DISTINCT) is exact",
   .met "Aggregate.exact_distinct_is_cardinality"⟩,
  ⟨"COUNT(DISTINCT) is O(1) in memory",
   .failing
     "the model threads the visited set as a List, one entry per distinct value; the Rust DistinctExact is O(1) but the refinement is unproved"
     "AggregateObligations.model_visited_set_length_is_the_count"⟩,
  ⟨"COUNT(DISTINCT) merges across shards",
   .failing
     "per-shard counts cannot be summed; a value on every shard is one distinct value"
     "AggregateObligations.sum_of_shard_counts_overcounts"⟩,
  ⟨"COUNT(ESTIMATE_DISTINCT) is a lower bound",
   .met "Aggregate.estimate_le_exact"⟩,
  ⟨"COUNT(ESTIMATE_DISTINCT) has a relative error bound",
   .failing
     "only monotonicity is proved; the quantitative bound is unproved, and exactness is refuted"
     "AggregateObligations.exactness_does_not_hold"⟩,
  ⟨"COUNT(ESTIMATE_DISTINCT) merges across shards",
   .failing
     "Bloom bits do not determine a count, so merged filters cannot yield a merged estimate; needs HLL or a re-scan"
     "AggregateObligations.bloom_bits_do_not_determine_the_count"⟩,
  ⟨"CQL surface accepts COUNT(DISTINCT c)",
   .failing
     "no AST node and no planner arm; the router recognises COUNT by name-matching only"
     "none (implementation item)"⟩,
  ⟨"CQL surface accepts COUNT(ESTIMATE_DISTINCT c)",
   .failing
     "same, and the function name does not exist in CQL, so it needs a Ferrosa-specific keyword"
     "none (implementation item)"⟩
]

/-- The count of red items, so a CI check can assert it only ever goes down. -/
def redCount : Nat := ledger.countP (fun i => match i.status with | .failing _ _ => true | _ => false)

/-- Six of the ledger's ten items are red, and the count is pinned here so a CI
    check can require it to fall and never to rise. -/
theorem ledger_has_six_red_items : redCount = 6 := by decide

/-- Render the ledger: `lake env lean --run` prints it, `#eval` shows it in an editor. -/
def renderAll : String :=
  String.intercalate "\n\n" (ledger.map render)

def main : IO Unit := do
  IO.println s!"CQL aggregate obligations ({ledger.length} items, {redCount} red)\n"
  IO.println renderAll

end Ferrosa.Cql.AggregateObligations
