/-
  Ferrosa formal specification — CQL front end, part 5: one system at two
  resolutions.
  ======================================================================

  The compare-and-set guarantee is proved on a scalar register, where each key
  holds one value, while the serializability check speaks about a keyspace in which
  each key holds the list of everything appended to it. A cell is what is left when
  only the last element of each list is kept, and the two are the same system seen at
  two resolutions — so a statement about a value and a statement about a history are
  statements about one implementation.

  Appending does not commute with that reading, because the list keeps history the
  cell forgets: two different lists are the same cell. The transition does commute
  — appending `v` at a key is setting that key to `v` and leaving the others alone —
  which is what makes the correspondence a refinement rather than a restatement. A
  whole conditional write refines its scalar execution in both branches: applied, or
  rejected with the keyspace unchanged.
-/
import Ferrosa.Cql.Lwt

set_option linter.deprecated false
set_option linter.unusedSimpArgs false

namespace Ferrosa.Cql.Refine

open Ferrosa
open Ferrosa.Accord

abbrev LCell := Ferrosa.Cql.Lwt.Cell
abbrev LWT := Ferrosa.Cql.Lwt.Lwt

/-- `SET k = v` on a cell, i.e. `Ferrosa.Cql.Lwt.Cell.set`. -/
def cellSet (c : LCell) (k : Key) (v : Option Val) : LCell := Ferrosa.Cql.Lwt.Cell.set c k v

/-- The CAS condition, i.e. `Ferrosa.Cql.Lwt.Lwt.cond`. -/
def lwtCond (c : LCell) (l : LWT) : Bool := Ferrosa.Cql.Lwt.Lwt.cond c l

/-- Execute a CAS, i.e. `Ferrosa.Cql.Lwt.Lwt.execute`. -/
def lwtExec (c : LCell) (l : LWT) : LCell × Bool := Ferrosa.Cql.Lwt.Lwt.execute c l

/-- The local `SET` and the specification's are the same operation, by definition: the refinement
    wrapper adds no behaviour of its own, only a name in this module's vocabulary. -/
@[simp] theorem cellSet_eq (c : LCell) (k : Key) (v : Option Val) :
    cellSet c k v = Ferrosa.Cql.Lwt.Cell.set c k v := rfl

/-- The local condition test is the specification's, by definition — so a conditional statement's
    decision here is the same decision the transaction spec makes there. -/
@[simp] theorem lwtCond_eq (c : LCell) (l : LWT) :
    lwtCond c l = Ferrosa.Cql.Lwt.Lwt.cond c l := rfl

/-- Executing a conditional here is executing it there, by definition; the pair returned is the
    cell and whether the condition held. -/
@[simp] theorem lwtExec_eq (c : LCell) (l : LWT) :
    lwtExec c l = Ferrosa.Cql.Lwt.Lwt.execute c l := rfl

/-- The last element of a key's list is the scalar value the cell model sees. -/
def State.last (s : State) (k : Key) : Option Val := (s k).getLast?

/-- The abstraction relation: `s`'s last values are `c`.  Everything here is stated
    in terms of this, never in terms of state equality. -/
def Simulates (s : State) (c : LCell) : Prop := ∀ k, State.last s k = c k

/-! ## The encoding is a section of the abstraction

A cell embeds in the keyspace as a singleton list at every key, and reading the last element back
returns the cell exactly, so nothing the scalar model can express is lost on the way in.
`decode_unique` adds that any two states which simulate the same cell agree on it, which makes the
abstraction determined by the keyspace rather than chosen for convenience. That is what lets a
property proved of a cell be read as a property of the keyspace it encodes.

-/

/-- Embed a cell into the keyspace: `none` is the empty list, `some v` a singleton.
    A cell carries no history, so the encoding has none to lose. -/
def encode (c : LCell) : State :=
  fun k => match c k with
    | none => []
    | some v => [v]

/-- Read a cell out of a keyspace. -/
def decode (s : State) : LCell := fun k => State.last s k

/-- Encoding a cell that holds nothing at a key gives a keyspace whose list for that
    key is empty, so its last value is nothing. -/
theorem last_encode_none (c : LCell) (k : Key) (h : c k = none) :
    State.last (encode c) k = none := by
  simp [encode, State.last, h]

/-- Encoding a cell that holds `v` at a key gives a keyspace whose last value at that
    key is `v`. -/
theorem last_encode_some (c : LCell) (k : Key) (v : Val) (h : c k = some v) :
    State.last (encode c) k = some v := by
  simp [encode, State.last, h]

/-- **`encode` simulates the cell it came from.** -/
theorem encode_simulates (c : LCell) : Simulates (encode c) c := by
  intro k
  cases h : c k with
  | none => exact last_encode_none c k h
  | some v => exact last_encode_some c k v h

/-- **The abstraction is a retraction**: decoding an encoded cell returns the cell
    exactly — the direction that says the encoding loses nothing the scalar model
    can express. -/
theorem decode_encode (c : LCell) : decode (encode c) = c := by
  funext k
  exact encode_simulates c k

/-- **The abstraction is well defined**: any two states that simulate a cell agree
    on it, so `decode` is *the* abstraction, not one choice among several. -/
theorem decode_unique (s : State) (c₁ c₂ : LCell)
    (h₁ : Simulates s c₁) (h₂ : Simulates s c₂) : c₁ = c₂ := by
  funext k
  rw [← h₁ k, ← h₂ k]

/-- Every state simulates its own decoding — the relation is total on the right. -/
theorem simulates_decode (s : State) : Simulates s (decode s) := fun _ => rfl

/-! ## Appending refines writing

Appending `v` at a key in the keyspace is setting that key to `v` in the cell: the list's last element
becomes `v` and every other key is left alone, so a state that simulated a cell still simulates the
cell it becomes. `simulates_append` is that step-by-step correspondence and `simulates_append_far`
keeps the keys the cell does not look at unchanged. One append does not reproduce the list's history,
but it does reproduce every question the scalar model can ask of it.

-/

/-- Running a single op is stepping once. -/
theorem State.runOps_singleton (s : State) (op : Op) : s.runOps [op] = (s.step op).1 := rfl

/-- The append list grows by exactly one element at the end. -/
theorem getLast?_append_singleton (l : List Val) (v : Val) : (l ++ [v]).getLast? = some v := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
    cases xs with
    | nil => rfl
    | cons y ys => exact ih

/-- An append to `k` leaves every other key alone. -/
theorem step_append_other (s : State) (k j : Key) (v : Val) (h : j ≠ k) :
    (s.step (.append k v)).1 j = s j := by
  simp [State.step, h]

/-- An append to `k` makes `v` the last element of its list. -/
theorem step_append_self (s : State) (k : Key) (v : Val) :
    (s.step (.append k v)).1 k = s k ++ [v] := by
  simp [State.step]

/-- **The transition refines.**  Appending `v` at `k` in the keyspace simulates
    setting `k` to `v` in the cell: for `k` because the last element is now `v`, for
    every other key because nothing moved. -/
theorem simulates_append (s : State) (c : LCell) (h : Simulates s c) (k : Key) (v : Val) :
    Simulates (s.runOps [.append k v]) (cellSet c k (some v)) := by
  intro j
  by_cases hj : j = k
  · rw [hj]
    show (s.runOps [Op.append k v] k).getLast? = cellSet c k (some v) k
    rw [State.runOps_singleton, step_append_self]
    simp [cellSet, Ferrosa.Cql.Lwt.Cell.set, getLast?_append_singleton]
  · show (s.runOps [Op.append k v] j).getLast? = cellSet c k (some v) j
    rw [State.runOps_singleton, step_append_other s k j v hj]
    simp only [cellSet, Ferrosa.Cql.Lwt.Cell.set, if_neg hj]
    exact h j

/-- Frames survive: a state still simulates a cell after an append somewhere the
    cell does not look. -/
theorem simulates_append_far (s : State) (c : LCell) (h : Simulates s c)
    (k j : Key) (v : Val) (hjk : j ≠ k) : State.last (s.runOps [.append k v]) j = c j := by
  show (s.runOps [Op.append k v] j).getLast? = c j
  rw [State.runOps_singleton, step_append_other s k j v hjk]
  exact h j

/-! ## Whole lightweight transactions refine

A whole conditional write refines its scalar execution in both branches: applied, the keyspace appends
the value and simulates the written cell; rejected, the keyspace is untouched and still simulates the
cell that did not change. `lwt_refines` is that statement and `failed_lwt_leaves_keyspace` is the abort
rule at keyspace level, so a rejected compare-and-set cannot make a replica diverge. This is what lets
`at_most_one_applies` and `serializableB` be two statements about one system rather than two systems.

-/

/-- The condition, evaluated against the keyspace's last values. -/
def condOn (s : State) (l : LWT) : Bool := State.last s l.key == l.expected

/-- The keyspace-level transition: test, and append only if the condition holds. -/
def executeOn (s : State) (l : LWT) : State :=
  if condOn s l then s.runOps [.append l.key l.newValue] else s

/-- The condition agrees across the two models whenever they simulate each other:
    the check the CAS performs on the cell is the check it performs on the
    keyspace. -/
theorem condOn_eq_cond (s : State) (c : LCell) (h : Simulates s c) (l : LWT) :
    condOn s l = lwtCond c l := by
  simp only [condOn, lwtCond_eq, Ferrosa.Cql.Lwt.Lwt.cond, h l.key]

/-- **An LWT refines its cell-level execution.**  Whichever branch is taken, the
    resulting keyspace simulates the resulting cell.  This is the theorem that lets
    `at_most_one_applies` (a cell statement) and `serializableB` (a keyspace check)
    be statements about one system. -/
theorem lwt_refines (s : State) (c : LCell) (h : Simulates s c) (l : LWT) :
    Simulates (executeOn s l) ((lwtExec c l).1) := by
  by_cases hc : lwtCond c l = true
  · have hOn : condOn s l = true := by rw [condOn_eq_cond s c h l]; exact hc
    have hc2 : Ferrosa.Cql.Lwt.Lwt.cond c l = true := by simpa using hc
    have hex : (lwtExec c l).1 = cellSet c l.key (some l.newValue) := by
      unfold lwtExec Ferrosa.Cql.Lwt.Lwt.execute
      simp [hc2]
    have hon : executeOn s l = s.runOps [.append l.key l.newValue] := by
      rw [executeOn, if_pos hOn]
    rw [hon, hex]
    exact simulates_append s c h l.key l.newValue
  · have hc' : lwtCond c l = false := by simpa using hc
    have hOn : condOn s l = false := by rw [condOn_eq_cond s c h l]; exact hc'
    have hc2' : Ferrosa.Cql.Lwt.Lwt.cond c l = false := by simpa using hc'
    have hex : (lwtExec c l).1 = c := by
      unfold lwtExec Ferrosa.Cql.Lwt.Lwt.execute
      simp [hc2']
    have hon : executeOn s l = s := by
      rw [executeOn, hOn]
      exact if_neg (by decide)
    rw [hon, hex]
    exact h

/-- A failed LWT leaves the keyspace unchanged — the abort rule at the keyspace
    level, so a rejected CAS cannot make a replica diverge. -/
theorem failed_lwt_leaves_keyspace (s : State) (l : LWT) (h : condOn s l = false) :
    executeOn s l = s := by
  rw [executeOn, h]
  exact if_neg (by decide)

end Ferrosa.Cql.Refine
