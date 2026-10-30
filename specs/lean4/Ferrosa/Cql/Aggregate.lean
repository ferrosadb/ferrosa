/-
  Ferrosa formal specification — CQL front end, part 8: three counts that are not
  the same function.
  ========================================================================

  `COUNT(*)` counts rows, `COUNT(DISTINCT v)` counts distinct values, and
  `COUNT(ESTIMATE_DISTINCT v)` returns a lower bound that can be short. Two are
  exact and one is not, and they are kept apart by name and by type because a
  caller handed the estimate where a count was asked for receives a wrong answer
  rather than a conservative one.

  `COUNT(*)` keeps a single counter and retains no row, so its state is a number
  however many rows pass through. `COUNT(DISTINCT v)` is exact and keeps the
  values it has seen beside the counter, which is the disk-backed map in
  `ferrosa-storage/src/count.rs`. The estimate keeps a fixed-size Bloom filter, so
  its memory does not grow with the data — the same bounded state that makes it a
  bound and not a count.
-/
import Ferrosa.Cql.Collections

namespace Ferrosa.Cql.Aggregate

open Ferrosa

/-- Values are `Nat`, restated here so this module does not depend on which layer's
    alias is in scope. -/
abbrev Val := Nat

/-- The aggregate functions the surface distinguishes. -/
inductive Agg where
  /-- `COUNT(*)` -/
  | count
  /-- `COUNT(DISTINCT v)` — exact by definition. -/
  | countDistinct
  /-- `COUNT(ESTIMATE_DISTINCT v)` — a lower bound, a different function. -/
  | estimateDistinct
  deriving DecidableEq, Repr

/-- Whether the function's result is exact. The estimate is not, which is why it has
    its own name and its own type. -/
def Agg.isExact : Agg → Bool
  | .count => true
  | .countDistinct => true
  | .estimateDistinct => false

/-- The estimate is the only one whose answer may be short. -/
theorem estimate_is_the_only_inexact :
    Agg.isExact .estimateDistinct = false
      ∧ Agg.isExact .countDistinct = true
      ∧ Agg.isExact .count = true := by
  decide

/-! ## `COUNT(*)` — an accumulator that retains no row

A `COUNT(*)` over a list of rows is the length of that list plus whatever the accumulator started
at, so the answer is exact and costs one addition per row. Two different row lists of the same
length produce the same accumulator, which is what a state of a single `Nat` means for an operator:
a count that has passed a million rows holds one number, and no row it has seen can be recovered
from it.

-/

/-- One counter, one step per row. The state is a `Nat`; there is nowhere for a row
    to be kept. -/
def countStep (n : Nat) (_row : Val) : Nat := n + 1

def foldCount : List Val → Nat → Nat
  | [], n => n
  | r :: rest, n => foldCount rest (countStep n r)

/-- **The accumulator equals the row count.** -/
theorem foldCount_eq (rows : List Val) (n : Nat) : foldCount rows n = n + rows.length := by
  induction rows generalizing n with
  | nil => simp [foldCount]
  | cons r rest ih => simp [foldCount, countStep, ih]; omega

/-- **It forgets which rows.**  Two different row lists of the same length produce the
    same accumulator, so the result carries no information about any individual row.
    That is the formal content of "it only updates an accumulator". -/
theorem count_forgets_which_rows (a b : List Val) (h : a.length = b.length) :
    foldCount a 0 = foldCount b 0 := by
  rw [foldCount_eq, foldCount_eq, h]

/-! ## `COUNT(DISTINCT v)` — exact, with the visited set kept outside the accumulator

The implementation streams each value to a disk-backed indexed map. Here the map's
contents are threaded as a `List` beside the counter — *beside*, not inside: the
counter is a `Nat`, and everything that grows is the map. -/

/-- One pass over the rows, carrying the distinct values seen so far and the count. -/
def walkExact : List Val → Nat → List Val → Nat
  | [], n, _ => n
  | v :: rest, n, seen =>
      if seen.contains v then walkExact rest n seen
      else walkExact rest (n + 1) (seen ++ [v])

/-- The invariant that carries the count: after `walkExact`, the counter equals the
    size of the visited set it ended with. Stated on the pair, because the counter and
    the set move together. -/
def walkExactPair : List Val → Nat × List Val → Nat × List Val
  | [], acc => acc
  | v :: rest, (n, seen) =>
      if seen.contains v then walkExactPair rest (n, seen)
      else walkExactPair rest (n + 1, seen ++ [v])

/-- The counter and the visited set stay in step: whenever they start equal, the
    running count after a pass over the rows is the size of the set it ends with. -/
theorem walkExactPair_fst (rows : List Val) (n : Nat) (seen : List Val)
    (h : n = seen.length) : (walkExactPair rows (n, seen)).1 = (walkExactPair rows (n, seen)).2.length := by
  induction rows generalizing n seen with
  | nil => simpa [walkExactPair] using h
  | cons v rest ih =>
      simp only [walkExactPair]
      by_cases hc : seen.contains v = true
      · simp only [hc, if_true]; exact ih n seen h
      · simp only [hc, if_false, List.length_append, List.length_singleton]
        exact ih (n + 1) (seen ++ [v]) (by simp [List.length_append]; omega)

/-- **`COUNT(DISTINCT)` from the empty map is the cardinality of the values seen.** -/
theorem exact_distinct_is_cardinality (rows : List Val) :
    (walkExactPair rows (0, [])).1 = (walkExactPair rows (0, [])).2.length :=
  walkExactPair_fst rows 0 [] rfl

/-! ## `COUNT(ESTIMATE_DISTINCT v)` — a Bloom filter, and a lower bound

The estimate's state is `Fin m → Bool`, `m` bits fixed at construction, so its memory does not grow
with the data however many rows pass through. A Bloom filter has no false negatives and a set bit is
never cleared, so a distinct value that has been counted stays counted and the answer can come up
short but never long. `estimate_le_exact` makes that one-sidedness a theorem: over the same rows the
estimate never exceeds the exact count.

-/

/-- A Bloom filter over `m` bits. The state's *size* is the type: `Fin m → Bool`, fixed
    at construction. -/
abbrev Bloom (m : Nat) := Fin m → Bool

def Bloom.empty {m : Nat} : Bloom m := fun _ => false

/-- Set the bits a value probes. -/
def Bloom.insert {m : Nat} (b : Bloom m) (probes : List (Fin m)) : Bloom m :=
  fun i => b i || probes.contains i

/-- The bits a value probes are all set. -/
def Bloom.member {m : Nat} (b : Bloom m) (probes : List (Fin m)) : Bool :=
  probes.all fun i => b i

/-- Inserting sets the probed bits. -/
theorem Bloom.insert_mem {m : Nat} (b : Bloom m) (probes : List (Fin m)) (i : Fin m)
    (h : i ∈ probes) : b.insert probes i = true := by
  have hc : probes.contains i = true := List.contains_iff_mem.mpr h
  simp only [Bloom.insert, hc, Bool.or_true]

/-- **A Bloom filter has no false negatives.**  This is the theorem the estimate's
    direction rests on. -/
theorem Bloom.no_false_negatives {m : Nat} (b : Bloom m) (probes : List (Fin m)) :
    (b.insert probes).member probes = true := by
  unfold Bloom.member
  rw [List.all_eq_true]
  intro i hi
  exact Bloom.insert_mem b probes i hi

/-- Setting bits cannot unset them: membership is monotone in the state. -/
theorem Bloom.member_mono {m : Nat} {b : Bloom m} {probes : List (Fin m)}
    (h : b.member probes = true) (extra : List (Fin m)) : (b.insert extra).member probes = true := by
  unfold Bloom.member at *
  rw [List.all_eq_true] at *
  intro i hi
  have := h i hi
  simp [Bloom.insert, this]

/-- One pass carrying both answers: the exact count (with its visited set) and the
    estimate (with its filter). The four cases are the crux — the impossible one is
    "the filter says seen, but the visited set does not contain it". -/
def walkFrom {m : Nat} (h : Val → List (Fin m)) :
    List Val → Nat → List Val → Nat → Bloom m → Nat × Nat
  | [], e, _, est, _ => (e, est)
  | v :: rest, e, seen, est, b =>
      if seen.contains v then
        if b.member (h v) then walkFrom h rest e seen est b
        else walkFrom h rest e seen (est + 1) (b.insert (h v))
      else
        if b.member (h v) then walkFrom h rest (e + 1) (seen ++ [v]) est b
        else walkFrom h rest (e + 1) (seen ++ [v]) (est + 1) (b.insert (h v))

/-- **The invariant that makes the estimate a lower bound.**  Whenever everything in
    the visited set is still a member of the filter, the estimate has not overtaken the
    exact count. -/
theorem walkFrom_estimate_le {m : Nat} (h : Val → List (Fin m)) (rows : List Val)
    (e est : Nat) (seen : List Val) (b : Bloom m)
    (hle : est ≤ e) (hall : ∀ v ∈ seen, b.member (h v) = true) :
    (walkFrom h rows e seen est b).2 ≤ (walkFrom h rows e seen est b).1 := by
  induction rows generalizing e est seen b with
  | nil => simpa [walkFrom] using hle
  | cons v rest ih =>
      rw [walkFrom]
      by_cases hc : seen.contains v = true
      · rw [if_pos hc]
        by_cases hb : b.member (h v) = true
        · -- Seen, and still a member: neither counter moves.
          rw [if_pos hb]
          exact ih e est seen b hle hall
        · -- Seen, but the filter disagrees. `hall` forbids this.
          have hmem : v ∈ seen := List.contains_iff_mem.mp hc
          exact absurd (hall v hmem) hb
      · rw [if_neg hc]
        by_cases hb : b.member (h v) = true
        · -- New, but the filter already claims it: a false positive. The estimate
          -- stays put while the exact count advances, so the gap can only shrink.
          rw [if_pos hb]
          refine ih (e + 1) est (seen ++ [v]) b (by omega) ?_
          intro w hw
          rw [List.mem_append] at hw
          rcases hw with hw | hw
          · exact hall w hw
          · rw [List.mem_singleton] at hw
            rw [hw]
            exact hb
        · -- Genuinely new: both counters advance, so the gap is preserved.
          rw [if_neg hb]
          refine ih (e + 1) (est + 1) (seen ++ [v]) (b.insert (h v)) (by omega) ?_
          intro w hw
          rw [List.mem_append] at hw
          rcases hw with hw | hw
          · exact Bloom.member_mono (hall w hw) (h v)
          · rw [List.mem_singleton] at hw
            rw [hw]
            exact Bloom.no_false_negatives b (h v)

/-- **The estimate is a lower bound on the exact count.** -/
theorem estimate_le_exact {m : Nat} (h : Val → List (Fin m)) (rows : List Val) :
    (walkFrom h rows 0 [] 0 Bloom.empty).2 ≤ (walkFrom h rows 0 [] 0 Bloom.empty).1 := by
  refine walkFrom_estimate_le h rows 0 0 ([] : List Val) Bloom.empty (by omega) ?_
  intro w hw
  simp at hw

/-! ## Why the two names cannot be swapped

`estimate_le_exact` alone allows equality, so it does not by itself forbid substituting
one answer for the other. This does: there is a row set and a probe assignment where the
estimate is **strictly** short — a false positive hides a distinct value — so an
estimate returned under the exact function's name would be a wrong answer, not a
conservative one. -/

/-- Every value probes bit 0: a filter that collides on everything. -/
def collidingHash : Val → List (Fin 8) := fun _ => [0]

/-- Two distinct values with disjoint probes: an honest filter. -/
def honestHash : Val → List (Fin 8) := fun v => if v = 0 then [0] else [1]

/-- Two distinct values colliding on the same single probe: `walkFrom` counts one, the
    exact walk counts two. -/
theorem estimate_can_be_strict :
    (walkFrom collidingHash [0, 1] 0 [] 0 Bloom.empty).2
      < (walkFrom collidingHash [0, 1] 0 [] 0 Bloom.empty).1 := by
  decide

/-- **The separation, as a theorem.**  There is an input on which the two functions
    disagree strictly, so they are different functions and may not share a name — which
    is what `Agg.isExact` records in the surface and what `DistinctEstimate` versus
    `DistinctExact` records in the types. -/
theorem exact_and_estimate_not_interchangeable :
    ∃ (h : Val → List (Fin 8)) (rows : List Val),
      (walkFrom h rows 0 [] 0 Bloom.empty).2 < (walkFrom h rows 0 [] 0 Bloom.empty).1 :=
  ⟨collidingHash, [0, 1], estimate_can_be_strict⟩

/-- And on an input with no collision the two agree, so the gap is the false-positive
    rate rather than a defect: `estimate = exact` when the filter is honest. -/
theorem estimate_exact_without_collision :
    (walkFrom honestHash [0, 1] 0 [] 0 Bloom.empty).2
      = (walkFrom honestHash [0, 1] 0 [] 0 Bloom.empty).1 := by
  decide

end Ferrosa.Cql.Aggregate
