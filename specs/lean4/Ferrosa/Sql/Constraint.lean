/-
  Ferrosa formal specification — column and table constraints on the PostgreSQL front end.
  ========================================================================================

  `NOT NULL` and `CHECK` are promises about every stored row. A promise that is parsed and
  then not enforced is the worst outcome: the client believes the table holds no NULL (or
  no row with `priority < 0`) and it does. Ferrosa used to parse a column's `NOT NULL` in
  `CREATE TABLE` and drop it, so a NULL was stored silently
  (`unenforced_not_null_stores_a_null`).

  The rule enforced now: a write is admitted only if the row it produces satisfies every
  constraint of its table; otherwise it is refused (`23502` for `NOT NULL`, `23514` for
  `CHECK`) and nothing is written. Then a table whose rows all satisfied its constraints
  still does after any admitted write (`admitted_writes_preserve_constraints`).

  Adding a constraint to a table that already has rows: a validated `ADD CONSTRAINT … CHECK`
  / `SET NOT NULL` checks the existing rows first and is refused if one fails. `NOT VALID`
  skips that check — so old rows may violate it — but every NEW write is still checked
  (`not_valid_still_checks_new_writes`). SQL's three-valued logic applies to `CHECK`: a check
  that evaluates to NULL (unknown) passes, as in PostgreSQL (`unknown_check_passes`).

  Source: `ferrosa-postgres/src/pg_constraint.rs`.
-/

namespace Ferrosa.Sql.Constraint

/-- A row: column values, `none` for SQL NULL. -/
abbrev Row := List (Option Nat)

/-- A constraint judges a row: `some true` holds, `some false` fails, `none` unknown. -/
abbrev Check := Row → Option Bool

/-- `NOT NULL` on column `i`. -/
def notNull (i : Nat) : Check := fun r => some ((r.getD i none).isSome)

/-- A row satisfies a constraint unless it evaluates to false: unknown passes (SQL). -/
def satisfies (c : Check) (r : Row) : Bool := c r != some false

/-- A row satisfies a list of constraints. -/
def satisfiesAll (cs : List Check) (r : Row) : Bool := cs.all (fun c => satisfies c r)

/-- Admit a write: refused (`none`) unless the new row satisfies every constraint. -/
def admit (cs : List Check) (table : List Row) (r : Row) : Option (List Row) :=
  if satisfiesAll cs r then some (r :: table) else none

/-- **Admitted writes preserve the constraints.**  If every stored row satisfied them, every
    row after an admitted write still does. -/
theorem admitted_writes_preserve_constraints (cs : List Check) (table table' : List Row)
    (r : Row) (hall : ∀ x ∈ table, satisfiesAll cs x = true)
    (h : admit cs table r = some table') : ∀ x ∈ table', satisfiesAll cs x = true := by
  unfold admit at h
  by_cases hr : satisfiesAll cs r = true
  · simp only [hr, ↓reduceIte, Option.some.injEq] at h
    subst h
    intro x hx
    simp at hx
    cases hx with
    | inl hx => subst hx; exact hr
    | inr hx => exact hall x hx
  · simp [hr] at h

/-- A write that violates a constraint is refused and writes nothing. -/
theorem violating_write_is_refused (cs : List Check) (table : List Row) (r : Row)
    (h : satisfiesAll cs r = false) : admit cs table r = none := by
  simp [admit, h]

/-- **The bug, as it was.**  With `NOT NULL` dropped from the constraint list, a row whose
    column is NULL is admitted; with it kept, the same write is refused. -/
theorem unenforced_not_null_stores_a_null :
    admit [] [] [some 1, none] = some [[some 1, none]]
      ∧ admit [notNull 1] [] [some 1, none] = none := by
  decide

/-- **Unknown passes.**  A `CHECK` that evaluates to NULL admits the row, as in PostgreSQL. -/
theorem unknown_check_passes (table : List Row) (r : Row) :
    admit [fun _ => none] table r = some (r :: table) := by
  simp [admit, satisfiesAll, satisfies]

/-- **`NOT VALID` still checks new writes.**  Adding a constraint without validating old rows
    leaves them as they are, but a new write that violates it is refused. -/
theorem not_valid_still_checks_new_writes (c : Check) (cs : List Check) (table : List Row)
    (r : Row) (h : satisfies c r = false) : admit (c :: cs) table r = none := by
  simp [admit, satisfiesAll, h]

/-- A validated `ADD CONSTRAINT` is refused when an existing row fails it. -/
def addValidated (c : Check) (cs : List Check) (table : List Row) : Option (List Check) :=
  if table.all (satisfies c) then some (c :: cs) else none

/-- After a validated add, every stored row satisfies the new constraint. -/
theorem validated_add_holds_on_existing_rows (c : Check) (cs cs' : List Check)
    (table : List Row) (h : addValidated c cs table = some cs') :
    ∀ x ∈ table, satisfies c x = true := by
  unfold addValidated at h
  by_cases ht : table.all (satisfies c) = true
  · intro x hx
    exact List.all_eq_true.mp ht x hx
  · simp [ht] at h

end Ferrosa.Sql.Constraint
