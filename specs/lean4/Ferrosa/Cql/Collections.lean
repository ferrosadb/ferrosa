/-
  Ferrosa formal specification — CQL front end, part 6: collections.
  ================================================================

  `UPDATE t SET tags = tags + {'x'} WHERE k = 1` carries no `IF` clause, so the
  front end calls it a blind write — and it cannot be executed without reading
  the old value of `tags`. The classifier that decides which writes skip the read
  phase, built in `Ferrosa/Cql/Classify.lean`, looks only at the `IF` clause and
  never at the assignment terms.

  What that costs depends on the collection type, and the two answers are
  different. Adding to a set is commutative and idempotent, so a last-write-wins
  merge of two concurrent adds keeps both elements: merging is a union. Appending
  to a list is neither, so the same merge keeps one element, and a client that
  received two acknowledgements sees one of its two writes.
-/
import Ferrosa.Cql.Classify

namespace Ferrosa.Cql.Collections

open Ferrosa

/-- Values are `Accord.Val`, restated here because this module is about the front
    end and does not otherwise import the accord layer. -/
abbrev Val := Nat

/-! ## A set is a membership predicate; a list is a sequence

A set is modelled by what can be asked of it — whether a value is in it — so two sets that answer the
same way for every value are the same value, and no replica can be holding a different one. A list is
a sequence, so order and multiplicity are both observable and two replicas can hold the same elements
in a different order. That difference is the whole subject of what follows: the same merge rule is
safe for one representation and lossy for the other.

-/

/-- A set, as a characteristic function.  Membership is the only observable, which
    is what makes a set's merge a union rather than a choice. -/
abbrev SetVal := Val → Bool

def setEmpty : SetVal := fun _ => false

def setAdd (s : SetVal) (v : Val) : SetVal := fun w => s w || w == v

def setUnion (a b : SetVal) : SetVal := fun w => a w || b w

abbrev ListVal := List Val

def listSnoc (l : ListVal) (v : Val) : ListVal := l ++ [v]

/-! ## Adds commute; appends do not

The order in which two concurrent adds reach a set does not matter, and adding the same value twice
leaves the set as it was, so last-write-wins over sets keeps both elements and a retried add is
harmless. Appending has neither property: `listSnoc_not_comm` exhibits a list where `1` then `2` and
`2` then `1` are different results. A merge that is correct for one collection type is therefore
wrong for the other, which is why the type of the column decides what an unread merge of two writes
can preserve.

-/

/-- **Set adds commute** — the whole reason a set survives last-write-wins. -/
theorem setAdd_comm (s : SetVal) (a b : Val) : setAdd (setAdd s a) b = setAdd (setAdd s b) a := by
  funext w
  simp [setAdd, Bool.or_assoc, Bool.or_left_comm, Bool.or_comm]

/-- Set adds are idempotent, so a replayed add is harmless. -/
theorem setAdd_idem (s : SetVal) (a : Val) : setAdd (setAdd s a) a = setAdd s a := by
  funext w
  simp [setAdd]

/-- A set's merge is a union: order-free, and equal to the serial result. -/
theorem set_lww_is_order_free (s : SetVal) (a b : Val) :
    setUnion (setAdd s a) (setAdd s b) = setAdd (setAdd s a) b := by
  funext w
  simp only [setUnion, setAdd]
  by_cases h1 : s w = true <;> by_cases h2 : (w == a) = true <;>
    by_cases h3 : (w == b) = true <;> simp_all

/-- **List appends do not commute.**  Concrete witness: appending 1 then 2 is not
    appending 2 then 1. -/
theorem listSnoc_not_comm :
    ∃ (l : ListVal) (a b : Val), listSnoc (listSnoc l a) b ≠ listSnoc (listSnoc l b) a := by
  exact ⟨[], 1, 2, by decide⟩

/-! ## Last-write-wins loses an element of a list

The merge the storage layer performs for a plain column keeps the later value and discards the
earlier one, and for two concurrent appends to the same list that means one of them disappears.
`list_lww_loses_an_element` states the count: the merge keeps one element where serial execution
keeps two, so a client holding two acknowledgements sees one of its two writes. The same two appends
as a set lose nothing in either order, which is what `set_lww_loses_nothing` records — the loss is a
property of the column's type under this merge, not of the merge alone.

-/

/-- The merge the storage layer performs for a plain column: the higher timestamp
    wins, and the other value is gone.  Modelled as "the later one is kept". -/
def lwwList (_first second : ListVal) : ListVal := second

/-- Serial execution of two appends. -/
def serialList (l : ListVal) (a b : Val) : ListVal := listSnoc (listSnoc l a) b

/-- Did the value survive into both results?  `true` when both appends are visible. -/
def keepsBoth (l : ListVal) : Bool := [1, 2].all (fun v => l.contains v)

/-- **The lost update.**  Two concurrent `LIST` appends, merged by last-write-wins,
    keep one element; the serial execution keeps two.  This is what "collection
    update is a read-modify-write" costs when the front end calls it blind. -/
theorem list_lww_loses_an_element :
    keepsBoth (lwwList (listSnoc [] 1) (listSnoc [] 2)) = false
      ∧ keepsBoth (serialList [] 1 2) = true := by
  decide

/-- The same two appends as a **set**: nothing is lost, in either merge order. -/
theorem set_lww_loses_nothing :
    keepsBoth ([1, 2] : ListVal) = true
      ∧ setUnion (setAdd setEmpty 1) (setAdd setEmpty 2)
          = setUnion (setAdd setEmpty 2) (setAdd setEmpty 1) := by
  refine ⟨by decide, ?_⟩
  funext w
  simp only [setUnion, setAdd, setEmpty]
  by_cases h1 : (w == 1) = true <;> by_cases h2 : (w == 2) = true <;>
    simp_all [beq_iff_eq, Bool.or_comm]

/-! ## The classifier is blind to the assignment terms

An `UPDATE` that appends to a collection is taken for a blind write by the front-end classifier, and
the read it needs to compute the new value is never scheduled. `collection_append_classified_blind`
and `collection_append_must_read` state the two halves — blind by classification, unexecutable
without a read — and `blind_classification_is_term_blind` puts them into the single statement that
there is a write the fast path accepts and cannot perform. `Stmt.blindSem` is the rule the planner
owes, refusing the fast path whenever an assignment reads its own column, while
`blindSem_accepts_scalar` shows a plain scalar assignment still qualifies so ordinary writes keep the
shortcut.

-/

/-- Does any assignment read its own column (`c = c + …`, `c = c - …`)? -/
def Stmt.readsOwnColumn : Stmt → Bool
  | .update _ assigns _ _ _ =>
      assigns.any (fun a => match a.term with
        | .selfAppend _ => true
        | .selfRemove _ => true
        | _ => false)
  | _ => false

/-- `UPDATE t SET tags = tags + {'x'} WHERE k = 1` — a list append. -/
def listAppendStmt : Stmt :=
  .update "t" [⟨"tags", .selfAppend 7⟩] [⟨"k", "=", .literal 1⟩] false []

/-- **Classified blind.**  No `IF` clause, so `Stmt.blind` says the fast path is
    open — the classification never looks at `tags = tags + …`. -/
theorem collection_append_classified_blind : Stmt.blind listAppendStmt = true := by decide

/-- **But it must read.**  The statement cannot be executed without the old value
    of `tags`, which is exactly the read the blind classification assumes away. -/
theorem collection_append_must_read : Stmt.readsOwnColumn listAppendStmt = true := by decide

/-- **The inconsistency, as one statement.**  There is a statement that the
    front-end classifier calls blind and that cannot be executed without a read.
    `blind` is a property of the `IF` clause alone, and a collection append has none
    while still being a read-modify-write. -/
theorem blind_classification_is_term_blind :
    ∃ s : Stmt, Stmt.blind s = true ∧ Stmt.readsOwnColumn s = true :=
  ⟨listAppendStmt, collection_append_classified_blind, collection_append_must_read⟩

/-- A plain scalar assignment (`SET name = 'b'`) reads nothing of its own, so the
    blind classification is right for it — the classifier is not uniformly wrong,
    which is why the fix belongs at the term level rather than by disabling the
    fast path. -/
theorem scalar_assignment_is_genuinely_blind :
    Stmt.blind (.update "t" [⟨"name", .literal 0⟩] [⟨"k", "=", .literal 1⟩] false []) = true
      ∧ Stmt.readsOwnColumn (.update "t" [⟨"name", .literal 0⟩] [⟨"k", "=", .literal 1⟩]
          false []) = false := by
  decide

/-- **The rule the planner owes.**  A statement whose assignment reads its own
    column must not take the blind path, whatever its `IF` clause says. -/
def Stmt.blindSem : Stmt → Bool
  | s => s.blind && !Stmt.readsOwnColumn s

/-- The tightened classifier refuses a collection append, so `tags = tags + {'x'}`
    takes the read phase and cannot be acknowledged on the write path alone. -/
theorem blindSem_rejects_collection_append : Stmt.blindSem listAppendStmt = false := by decide

/-- A plain scalar assignment still qualifies as blind under the tightened
    classifier, so ordinary writes keep the fast path. -/
theorem blindSem_accepts_scalar :
    Stmt.blindSem (.update "t" [⟨"name", .literal 0⟩] [⟨"k", "=", .literal 1⟩] false []) = true := by
  decide

/-- `blindSem` never admits something `blind` rejects: it is a restriction of the
    implemented classifier, so tightening it cannot open a fast path that was
    closed. -/
theorem blindSem_is_stricter (s : Stmt) (h : Stmt.blindSem s = true) : Stmt.blind s = true := by
  simp only [Stmt.blindSem, Bool.and_eq_true] at h
  exact h.1

end Ferrosa.Cql.Collections
