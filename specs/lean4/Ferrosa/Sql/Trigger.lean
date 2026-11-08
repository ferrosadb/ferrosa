/-
  Ferrosa formal specification — AFTER ROW triggers: once per affected row, inside the transaction.
  =================================================================================================

  Decision D13 (`specs/todo/postgres-frontend/decisions.md`): `CREATE TRIGGER … AFTER INSERT |
  UPDATE [OF col] … FOR EACH ROW EXECUTE PROCEDURE f()` is stored with its table and fires from
  the PostgreSQL write path with `NEW`, `OLD` and `TG_OP` bound.

  PostgreSQL's contract (`doc/src/sgml/trigger.sgml`) is what is modelled: a row-level AFTER
  trigger is queued for each row the statement affected and fired once per queued row at the end
  of the statement, inside the statement's transaction, so its effects commit or roll back with
  it. The implementation (`ferrosa-postgres/src/procedural/triggers.rs`) collects one row event
  per written row in the DML executor, then — after the statement, before control returns —
  runs the trigger function for each event through the host interface, whose writes go into
  the same transaction write-set.

  What is not modelled: the function's own behaviour (its effect is an opaque write tagged by
  the row that fired it), and cascades (a trigger's write firing a trigger), which the
  implementation bounds by depth and this model does not need for the once-per-row claim.
-/
set_option linter.deprecated false

namespace Ferrosa.Sql.Trigger

/-- A row, by key. -/
abbrev Row := Nat

/-- A write in a transaction's write-set: the statement's own row write, or the effect of the
    trigger fired for a row. -/
inductive Write where
  | stmt (r : Row)
  | trig (r : Row)
  deriving DecidableEq, Repr

/-- The trigger queue for a statement: the affected rows (in the order written) for which the
    trigger's event matches — `INSERT`, or `UPDATE` with an `OF` column in the `SET` list. -/
def queue (rows : List Row) (fires : Row → Bool) : List Row := rows.filter fires

/-- A statement with triggers, appended to the transaction's write-set `ws`: its row writes,
    then the trigger effects for the queued rows. -/
def runStatement (ws : List Write) (rows : List Row) (fires : Row → Bool) : List Write :=
  ws ++ rows.map .stmt ++ (queue rows fires).map .trig

/-- The transaction's single decision applied to the store. -/
def decide (commit : Bool) (store ws : List Write) : List Write :=
  if commit then store ++ ws else store

/-- A statement affects each row at most once (the executor writes a row once). -/
abbrev Affected (rows : List Row) : Prop := rows.Nodup

/-- **Exactly once per affected row.** A row the statement affected and the trigger's event
    matches is fired exactly once. -/
theorem fires_exactly_once (rows : List Row) (fires : Row → Bool) (r : Row)
    (haff : Affected rows) (hr : r ∈ rows) (hm : fires r = true) :
    (queue rows fires).count r = 1 := by
  apply List.Nodup.count_of_mem
  · exact List.Nodup.sublist List.filter_sublist haff
  · exact List.mem_filter.mpr ⟨hr, hm⟩

/-- **Never for a row the statement did not affect.** -/
theorem never_fires_for_an_unaffected_row (rows : List Row) (fires : Row → Bool) (r : Row)
    (hr : r ∉ rows) : (queue rows fires).count r = 0 := by
  apply List.count_eq_zero.mpr
  intro h
  exact hr (List.mem_filter.mp h).1

/-- **Never when the event does not match** (an `UPDATE` that sets none of the `OF` columns). -/
theorem never_fires_when_the_event_does_not_match (rows : List Row) (fires : Row → Bool)
    (r : Row) (hm : fires r = false) : (queue rows fires).count r = 0 := by
  apply List.count_eq_zero.mpr
  intro h
  have := (List.mem_filter.mp h).2
  rw [hm] at this
  exact Bool.false_ne_true this

/-- **After the statement.** Every trigger effect comes after every row write of the statement
    that fired it (the queue is run at the end of the statement). -/
theorem triggers_follow_the_statement (ws : List Write) (rows : List Row) (fires : Row → Bool) :
    runStatement ws rows fires = (ws ++ rows.map .stmt) ++ (queue rows fires).map .trig := rfl

/-- Each queued row's effect is in the transaction's write-set. -/
theorem effect_is_in_the_write_set (ws : List Write) (rows : List Row) (fires : Row → Bool)
    (r : Row) (hq : r ∈ queue rows fires) : Write.trig r ∈ runStatement ws rows fires := by
  unfold runStatement
  rw [List.mem_append]
  right
  exact List.mem_map.mpr ⟨r, hq, rfl⟩

/-- **Rolled back with the transaction.** If the transaction does not commit, neither the
    statement's writes nor any trigger effect reach the store. -/
theorem rollback_discards_trigger_effects (store ws : List Write) (rows : List Row)
    (fires : Row → Bool) :
    decide false store (runStatement ws rows fires) = store := rfl

/-- **Committed with the transaction.** If it commits, every trigger effect reaches the store,
    together with the statement's writes. -/
theorem commit_keeps_trigger_effects (store ws : List Write) (rows : List Row)
    (fires : Row → Bool) (r : Row) (hq : r ∈ queue rows fires) :
    Write.trig r ∈ decide true store (runStatement ws rows fires) := by
  unfold decide
  simp only [ite_true]
  exact List.mem_append_right store (effect_is_in_the_write_set ws rows fires r hq)

/-- The two outcomes are the only ones: a trigger effect is in the store exactly when the
    transaction commits (given the store did not already hold it). -/
theorem trigger_effect_shares_the_decision (store ws : List Write) (rows : List Row)
    (fires : Row → Bool) (r : Row) (hq : r ∈ queue rows fires)
    (hfresh : Write.trig r ∉ store) (commit : Bool) :
    Write.trig r ∈ decide commit store (runStatement ws rows fires) ↔ commit = true := by
  cases commit with
  | false =>
    simp only [rollback_discards_trigger_effects, Bool.false_eq_true, iff_false]
    exact hfresh
  | true =>
    simp only [iff_true]
    exact commit_keeps_trigger_effects store ws rows fires r hq

/-! ## Cascades are bounded

A trigger's write can fire a trigger. The implementation refuses a cascade deeper than
`MAX_TRIGGER_DEPTH` with `54001`, and that bound — not the machine's stack — must be what ends
a self-firing trigger (found while building: at depth 16 the stack ran out first). The model:
each level either stops (the function writes nothing that fires) or fires one more level. -/

/-- How a cascade ends: it completed after `n` nested levels, or the bound refused it. -/
inductive Cascade where
  | done (levels : Nat)
  | refused
  deriving DecidableEq, Repr

/-- Run a cascade from `depth`; `refires d` says whether level `d`'s write fires again. -/
def cascade (bound : Nat) (refires : Nat → Bool) : Nat → Nat → Cascade
  | depth, 0 => if depth ≥ bound then .refused else .done depth
  | depth, fuel + 1 =>
      if depth ≥ bound then .refused
      else if refires depth then cascade bound refires (depth + 1) fuel
      else .done (depth + 1)

/-- **A self-firing trigger is refused, never unbounded.** With fuel past the bound, a trigger
    that always fires itself ends in `refused`. -/
theorem self_firing_cascade_is_refused (bound : Nat) :
    cascade bound (fun _ => true) 0 (bound + 1) = .refused := by
  suffices h : ∀ k d, d + k = bound → cascade bound (fun _ => true) d (k + 1) = .refused by
    simpa using h bound 0 (by omega)
  intro k
  induction k with
  | zero =>
    intro d hd
    have hb : d ≥ bound := by omega
    simp [cascade, hb]
  | succ k ih =>
    intro d hd
    have hlt : ¬ d ≥ bound := by omega
    simp only [cascade, hlt, ite_false, ite_true]
    exact ih (d + 1) (by omega)

/-- **A completed cascade never went deeper than the bound.** -/
theorem completed_cascade_respects_the_bound (bound : Nat) (refires : Nat → Bool) :
    ∀ fuel depth levels, cascade bound refires depth fuel = .done levels → levels ≤ bound := by
  intro fuel
  induction fuel with
  | zero =>
    intro depth levels h
    simp only [cascade] at h
    split at h
    · exact absurd h (by simp)
    · rename_i hb; injection h with h; omega
  | succ fuel ih =>
    intro depth levels h
    simp only [cascade] at h
    split at h
    · exact absurd h (by simp)
    · rename_i hb
      split at h
      · exact ih (depth + 1) levels h
      · injection h with h; omega

end Ferrosa.Sql.Trigger
