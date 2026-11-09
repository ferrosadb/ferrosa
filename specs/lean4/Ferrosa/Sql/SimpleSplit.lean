/-
  Ferrosa formal specification — splitting a simple query (found by W4).
  ======================================================================

  A simple `Query` message may hold several statements separated by `;`. The
  front end treated the whole message as one statement, so `SELECT 1; SELECT 2`
  failed with `42601 bad token: ;`. The splitter (`simple_split.rs`) scans with a
  quote state: a `;` inside a quoted region (string, quoted identifier,
  dollar-quoted body, comment — all one "inside" state here) does not separate.

  `split_rejoins` — joining the pieces with `;` gives the input back, so nothing
  is lost or invented. `pieces_have_no_top_level_semicolon` — no piece contains a
  separator outside quotes, i.e. the scanner never cuts inside a quoted region
  and never leaves a separator in a statement.
-/

namespace Ferrosa.Sql.SimpleSplit

/-- What the scanner distinguishes: a separator, a quote toggle, anything else. -/
inductive C where
  | semi
  | quote
  | other
  deriving DecidableEq, Repr

open C

/-- The scanner. `q` is the quote state, `cur` the piece so far. -/
def go : Bool → List C → List C → List (List C)
  | _, cur, [] => [cur]
  | q, cur, semi :: rest => if q then go q (cur ++ [semi]) rest else cur :: go q [] rest
  | q, cur, quote :: rest => go (!q) (cur ++ [quote]) rest
  | q, cur, other :: rest => go q (cur ++ [other]) rest

def split (xs : List C) : List (List C) := go false [] xs

def join : List (List C) → List C
  | [] => []
  | [p] => p
  | p :: ps => p ++ semi :: join ps

/-- Joining a piece in front of a non-empty list of pieces puts a separator between that piece
    and the rest. -/
theorem join_cons_ne (p : List C) (ps : List (List C)) (h : ps ≠ []) :
    join (p :: ps) = p ++ semi :: join ps := by
  cases ps with
  | nil => exact absurd rfl h
  | cons _ _ => rfl

/-- The scanner always yields at least one piece, even for empty input; the piece in progress is
    emitted at the end. -/
theorem go_ne_nil (q : Bool) (cur xs : List C) : go q cur xs ≠ [] := by
  induction xs generalizing q cur with
  | nil => simp [go]
  | cons c rest ih =>
    cases c with
    | semi => cases q <;> simp [go, ih]
    | quote => simp [go, ih]
    | other => simp [go, ih]

/-- The scanner loses nothing: the pieces, rejoined, are the prefix plus the input. -/
theorem go_rejoins (q : Bool) (cur xs : List C) : join (go q cur xs) = cur ++ xs := by
  induction xs generalizing q cur with
  | nil => simp [go, join]
  | cons c rest ih =>
    cases c with
    | semi =>
      cases q with
      | true => simp [go, ih]
      | false =>
        simp only [go, Bool.false_eq_true, ite_false]
        rw [join_cons_ne _ _ (go_ne_nil _ _ _), ih]
        simp
    | quote => simp [go, ih]
    | other => simp [go, ih]

/-- Joining the pieces of a split with `;` gives the original message back: splitting loses
    nothing and invents nothing. -/
theorem split_rejoins (xs : List C) : join (split xs) = xs := by
  simp [split, go_rejoins]

/-- Scanning `p` from quote state `q`, a separator is met outside quotes. -/
def topSemi : Bool → List C → Bool
  | _, [] => false
  | q, semi :: rest => !q || topSemi q rest
  | q, quote :: rest => topSemi (!q) rest
  | q, other :: rest => topSemi q rest

/-- The quote state after scanning `p` from `q`. -/
def stateAfter : Bool → List C → Bool
  | q, [] => q
  | q, quote :: rest => stateAfter (!q) rest
  | q, _ :: rest => stateAfter q rest

/-- Scanning a concatenation for a top-level separator is scanning the first part, then scanning
    the second part from the quote state the first part left. -/
theorem topSemi_append (q : Bool) (a b : List C) :
    topSemi q (a ++ b) = (topSemi q a || topSemi (stateAfter q a) b) := by
  induction a generalizing q with
  | nil => simp [topSemi, stateAfter]
  | cons c rest ih =>
    cases c <;> simp [topSemi, stateAfter, ih, Bool.or_assoc]

/-- Invariant: the piece so far has no top-level separator and ends in state `q`. -/
theorem go_pieces (q : Bool) (cur xs : List C)
    (hcur : topSemi false cur = false) (hq : stateAfter false cur = q) :
    ∀ p ∈ go q cur xs, topSemi false p = false := by
  induction xs generalizing q cur with
  | nil => intro p hp; simp [go] at hp; subst hp; exact hcur
  | cons c rest ih =>
    cases c with
    | semi =>
      cases q with
      | true =>
        apply ih true (cur ++ [semi])
        · rw [topSemi_append, hcur, hq]; simp [topSemi]
        · simp [stateAfter_append, hq, stateAfter]
      | false =>
        intro p hp
        simp only [go, Bool.false_eq_true, ite_false, List.mem_cons] at hp
        rcases hp with rfl | hp
        · exact hcur
        · exact ih false [] (by simp [topSemi]) (by simp [stateAfter]) p hp
    | quote =>
      apply ih (!q) (cur ++ [quote])
      · rw [topSemi_append, hcur, hq]; simp [topSemi]
      · simp [stateAfter_append, hq, stateAfter]
    | other =>
      apply ih q (cur ++ [other])
      · rw [topSemi_append, hcur, hq]; simp [topSemi]
      · simp [stateAfter_append, hq, stateAfter]
where
  stateAfter_append (q : Bool) (a b : List C) :
      stateAfter q (a ++ b) = stateAfter (stateAfter q a) b := by
    induction a generalizing q with
    | nil => rfl
    | cons c rest ih => cases c <;> simp [stateAfter, ih]

/-- No piece of a split contains a `;` outside a quoted region. The splitter never cuts inside a
    string, identifier, dollar-quoted body or comment, and never leaves a separator inside a
    statement. -/
theorem pieces_have_no_top_level_semicolon (xs : List C) :
    ∀ p ∈ split xs, topSemi false p = false :=
  go_pieces false [] xs (by simp [topSemi]) (by simp [stateAfter])

end Ferrosa.Sql.SimpleSplit
