/-
  Ferrosa formal specification — `LIMIT` / `OFFSET` with bound parameters.
  ========================================================================

  Oban claims jobs with `… LIMIT $2`, so a limit can arrive as a bound
  parameter rather than a literal. PostgreSQL's rules for the bound value:

  * `NULL` means no limit (`LIMIT NULL` is `LIMIT ALL`) and no offset;
  * a negative value is an error (`2201W` / `2201X`), never clamped to zero;
  * otherwise it is the count.

  The engine resolves the parameter once, when the statement is opened, and then
  runs the same operator it runs for a literal. These theorems pin that the
  resolved form behaves exactly as the literal form (`param_matches_literal`),
  that a limit never returns more rows than it names (`limit_bounds_length`),
  and that the result is always a contiguous slice of the input
  (`limit_offset_is_a_slice`), so a limit never reorders or invents rows.
-/

namespace Ferrosa.Sql.Limit

/-- The parameter values that matter here. -/
inductive P where
  | null
  | int (n : Int)
  deriving DecidableEq, Repr

inductive Err where
  | negative
  deriving DecidableEq, Repr

/-- Resolve a bound `LIMIT`/`OFFSET` value: `none` is "no bound". -/
def resolve : P → Except Err (Option Nat)
  | .null => .ok none
  | .int n => if n < 0 then .error .negative else .ok (some n.toNat)

/-- `OFFSET off LIMIT lim` over a row list (rows are opaque here). -/
def apply {α : Type} (off : Option Nat) (lim : Option Nat) (rows : List α) : List α :=
  let skipped := rows.drop (off.getD 0)
  match lim with
  | none => skipped
  | some n => skipped.take n

/-- A NULL parameter resolves to no bound, and applying no offset and no limit returns every
    row: `LIMIT NULL` is `LIMIT ALL`, as in PostgreSQL. -/
theorem null_is_no_limit {α : Type} (rows : List α) :
    resolve .null = .ok none ∧ apply none none rows = rows := by
  simp [resolve, apply]

/-- A negative parameter is an error, never clamped to zero, so `LIMIT -1` fails loudly instead
    of quietly returning nothing. -/
theorem negative_is_refused (n : Int) (h : n < 0) : resolve (.int n) = .error .negative := by
  simp [resolve, h]

/-- A non-negative parameter behaves exactly as the literal it carries. -/
theorem param_matches_literal {α : Type} (n : Nat) (off : Option Nat) (rows : List α) :
    resolve (.int n) = .ok (some n) ∧
      apply off (some n) rows = (rows.drop (off.getD 0)).take n := by
  simp [resolve, apply]

/-- A limit of `n` never returns more than `n` rows, whatever the offset and the input. -/
theorem limit_bounds_length {α : Type} (off : Option Nat) (n : Nat) (rows : List α) :
    (apply off (some n) rows).length ≤ n := by
  simp only [apply, List.length_take]
  exact Nat.min_le_left _ _

/-- The result is `rows` with a prefix and a suffix removed: a contiguous slice. -/
theorem limit_offset_is_a_slice {α : Type} (off lim : Option Nat) (rows : List α) :
    ∃ pre post, pre ++ apply off lim rows ++ post = rows := by
  refine ⟨rows.take (off.getD 0), ?_⟩
  cases lim with
  | none => exact ⟨[], by simp [apply]⟩
  | some n =>
    refine ⟨(rows.drop (off.getD 0)).drop n, ?_⟩
    simp only [apply]
    rw [List.append_assoc, List.take_append_drop, List.take_append_drop]

end Ferrosa.Sql.Limit
