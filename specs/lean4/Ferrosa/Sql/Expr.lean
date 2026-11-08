/-
  Ferrosa formal specification — SQL scalar expressions (W1 of the Oban work).
  ==========================================================================

  The PostgreSQL front end grows a general expression engine
  (`ferrosa-sql`'s `ScalarExpr`): projected expressions, column-to-column
  comparisons, functions, `IN`, `= ANY(array)`, `IS NULL`, `coalesce`, and
  subqueries. Postgrex's type bootstrap and Oban's queries need all of it
  (`specs/todo/postgres-frontend/oban-compat.md`).

  SQL evaluates boolean conditions in three-valued logic: a comparison with a
  NULL operand is UNKNOWN, and `WHERE` keeps a row only when its condition is
  TRUE. This file pins the semantics the engine must implement, in the shape
  the Rust evaluator has: values, a three-valued truth type, and an evaluator
  over a row. The facts proved here are the ones the implementation leans on:

  * The engine keeps its old fast path for `column <op> literal` (point reads,
    Accord read sets). `legacy_compare_agrees` shows that path and the general
    evaluator give the same truth value, so the fast path is an optimisation,
    never a different meaning.
  * `x = ANY(array)` is TRUE when some element equals `x`, otherwise UNKNOWN
    when `x` or any element is NULL, otherwise FALSE (`any_true_iff`,
    `any_false_iff`). Oban acknowledges a job with `state = ANY($4)`.
  * `x NOT IN (… NULL …)` is never TRUE (`not_in_with_null_never_true`), the
    classic trap an implementation that treats NULL as "not equal" falls into.
  * `coalesce` returns its first non-NULL argument (`coalesce_first_non_null`).
  * `WHERE` keeps exactly the rows whose condition is TRUE
    (`filter_keeps_only_true`); UNKNOWN rows are dropped, not kept.
  * The three-valued connectives satisfy De Morgan (`not_and`, `not_or`), which
    the planner may use when it pushes a negation down.
-/

namespace Ferrosa.Sql.Expr

/-! ## Values and truth -/

/-- The values this model needs: NULL, integers, booleans and strings. -/
inductive V where
  | null
  | int (n : Int)
  | bool (b : Bool)
  | str (s : String)
  deriving DecidableEq, Repr

/-- SQL truth: `some true`, `some false`, or `none` (UNKNOWN). -/
abbrev TV := Option Bool

def tvAnd : TV → TV → TV
  | some false, _ => some false
  | _, some false => some false
  | some true, some true => some true
  | _, _ => none

def tvOr : TV → TV → TV
  | some true, _ => some true
  | _, some true => some true
  | some false, some false => some false
  | _, _ => none

def tvNot : TV → TV
  | some b => some (!b)
  | none => none

/-- Equality of two values; UNKNOWN when either is NULL or the types differ. -/
def veq : V → V → TV
  | .null, _ => none
  | _, .null => none
  | .int a, .int b => some (a == b)
  | .bool a, .bool b => some (a == b)
  | .str a, .str b => some (a == b)
  | _, _ => none

/-- Strictly-less-than; UNKNOWN on NULL or mismatched types. -/
def vlt : V → V → TV
  | .int a, .int b => some (decide (a < b))
  | .str a, .str b => some (decide (a < b))
  | _, _ => none

inductive Cmp where
  | eq | ne | lt
  deriving DecidableEq, Repr

def vcmp : Cmp → V → V → TV
  | .eq, a, b => veq a b
  | .ne, a, b => tvNot (veq a b)
  | .lt, a, b => vlt a b

/-! ## Expressions and evaluation -/

/-- The fragment of `ScalarExpr` this file reasons about. -/
inductive E where
  | col (i : Nat)
  | lit (v : V)
  | cmp (op : Cmp) (a b : E)
  | and (a b : E)
  | or (a b : E)
  | not (a : E)
  | isNull (a : E)
  deriving Repr

abbrev Row := List V

def colOf (r : Row) (i : Nat) : V := r.getD i .null

def toV : TV → V
  | some b => .bool b
  | none => .null

def toTV : V → TV
  | .bool b => some b
  | _ => none

/-- The general evaluator, as the Rust `eval` does it: every node yields a value. -/
def eval (r : Row) : E → V
  | .col i => colOf r i
  | .lit v => v
  | .cmp op a b => toV (vcmp op (eval r a) (eval r b))
  | .and a b => toV (tvAnd (toTV (eval r a)) (toTV (eval r b)))
  | .or a b => toV (tvOr (toTV (eval r a)) (toTV (eval r b)))
  | .not a => toV (tvNot (toTV (eval r a)))
  | .isNull a => .bool (eval r a == .null)

/-- A condition's truth over a row. -/
def truth (r : Row) (e : E) : TV := toTV (eval r e)

/-- The legacy fast path: `column <op> literal`, compared directly. -/
def legacyCompare (r : Row) (i : Nat) (op : Cmp) (v : V) : TV :=
  vcmp op (colOf r i) v

theorem toTV_toV (t : TV) : toTV (toV t) = t := by
  cases t with
  | none => rfl
  | some b => rfl

/-- The fast path and the general evaluator agree on every row. -/
theorem legacy_compare_agrees (r : Row) (i : Nat) (op : Cmp) (v : V) :
    legacyCompare r i op v = truth r (.cmp op (.col i) (.lit v)) := by
  simp [legacyCompare, truth, eval, toTV_toV]

/-- `IS NULL` is never UNKNOWN. -/
theorem isNull_is_two_valued (r : Row) (e : E) : truth r (.isNull e) ≠ none := by
  simp [truth, eval, toTV]

/-! ## Three-valued connectives -/

theorem tvNot_tvNot (t : TV) : tvNot (tvNot t) = t := by
  cases t with
  | none => rfl
  | some b => cases b <;> rfl

theorem not_and (a b : TV) : tvNot (tvAnd a b) = tvOr (tvNot a) (tvNot b) := by
  cases a with
  | none => cases b with
    | none => rfl
    | some y => cases y <;> rfl
  | some x => cases x <;> cases b with
    | none => rfl
    | some y => cases y <;> rfl

theorem not_or (a b : TV) : tvNot (tvOr a b) = tvAnd (tvNot a) (tvNot b) := by
  cases a with
  | none => cases b with
    | none => rfl
    | some y => cases y <;> rfl
  | some x => cases x <;> cases b with
    | none => rfl
    | some y => cases y <;> rfl

/-! ## `= ANY(array)` and `NOT IN` -/

/-- `x = ANY(xs)`: the OR of `x = e` over the elements, starting from FALSE. -/
def anyEq (x : V) : List V → TV
  | [] => some false
  | e :: es => tvOr (veq x e) (anyEq x es)

/-- `x NOT IN (xs)` is `NOT (x = ANY(xs))`. -/
def notIn (x : V) (xs : List V) : TV := tvNot (anyEq x xs)

theorem tvOr_true_iff (a b : TV) : tvOr a b = some true ↔ a = some true ∨ b = some true := by
  cases a with
  | none => cases b with
    | none => simp [tvOr]
    | some y => cases y <;> simp [tvOr]
  | some x => cases x <;> cases b with
    | none => simp [tvOr]
    | some y => cases y <;> simp [tvOr]

theorem tvOr_false_iff (a b : TV) : tvOr a b = some false ↔ a = some false ∧ b = some false := by
  cases a with
  | none => cases b with
    | none => simp [tvOr]
    | some y => cases y <;> simp [tvOr]
  | some x => cases x <;> cases b with
    | none => simp [tvOr]
    | some y => cases y <;> simp [tvOr]

/-- TRUE exactly when some element compares equal. -/
theorem any_true_iff (x : V) (xs : List V) :
    anyEq x xs = some true ↔ ∃ e ∈ xs, veq x e = some true := by
  induction xs with
  | nil => simp [anyEq]
  | cons e es ih => simp [anyEq, tvOr_true_iff, ih]

/-- FALSE exactly when every element compares definitely unequal (so no NULL anywhere). -/
theorem any_false_iff (x : V) (xs : List V) :
    anyEq x xs = some false ↔ ∀ e ∈ xs, veq x e = some false := by
  induction xs with
  | nil => simp [anyEq]
  | cons e es ih => simp [anyEq, tvOr_false_iff, ih]

theorem veq_null_right (x : V) : veq x .null = none := by
  cases x <;> rfl

/-- A NULL in the list makes `NOT IN` never TRUE. -/
theorem not_in_with_null_never_true (x : V) (xs : List V) (h : V.null ∈ xs) :
    notIn x xs ≠ some true := by
  intro hnot
  have hfalse : anyEq x xs = some false := by
    unfold notIn at hnot
    cases hq : anyEq x xs with
    | none => simp [hq, tvNot] at hnot
    | some b => cases b <;> simp_all [tvNot]
  have := (any_false_iff x xs).mp hfalse .null h
  simp [veq_null_right] at this

/-! ### The evaluator's loop

  The Rust evaluator computes `IN (…)`, `= ANY(array)` and `IN (subquery)` (which
  the parser lowers to `= ANY(ARRAY(subquery))`, as PostgreSQL defines it) with a
  left fold that starts at FALSE and stops at the first TRUE. These show that
  loop is `anyEq`, and that stopping early never changes the answer.
-/

/-- The loop: `acc := acc OR (x = e)` over the elements, left to right. -/
def anyLoop (x : V) (acc : TV) : List V → TV
  | [] => acc
  | e :: es => anyLoop x (tvOr acc (veq x e)) es

theorem tvOr_true_left (b : TV) : tvOr (some true) b = some true := by
  cases b <;> rfl

/-- Once the accumulator is TRUE it stays TRUE, so breaking out is sound. -/
theorem anyLoop_true_absorbs (x : V) (xs : List V) : anyLoop x (some true) xs = some true := by
  induction xs with
  | nil => rfl
  | cons e es ih => simp [anyLoop, tvOr_true_left, ih]

theorem tvOr_assoc (a b c : TV) : tvOr (tvOr a b) c = tvOr a (tvOr b c) := by
  cases a with
  | none => cases b with
    | none => cases c with
      | none => rfl
      | some z => cases z <;> rfl
    | some y => cases y <;> cases c <;> try rfl
                all_goals (rename_i z; cases z <;> rfl)
  | some x => cases x <;> cases b <;> try rfl
              all_goals first
                | (rename_i y; cases y <;> cases c <;> try rfl
                   all_goals (rename_i z; cases z <;> rfl))
                | (cases c <;> try rfl
                   all_goals (rename_i z; cases z <;> rfl))

theorem tvOr_false_left (b : TV) : tvOr (some false) b = b := by
  cases b with
  | none => rfl
  | some y => cases y <;> rfl

theorem anyLoop_acc (x : V) (acc : TV) (xs : List V) :
    anyLoop x acc xs = tvOr acc (anyEq x xs) := by
  induction xs generalizing acc with
  | nil =>
    cases acc with
    | none => rfl
    | some b => cases b <;> rfl
  | cons e es ih => simp [anyLoop, anyEq, ih, tvOr_assoc]

/-- The loop from FALSE is exactly `anyEq`. -/
theorem anyLoop_is_anyEq (x : V) (xs : List V) : anyLoop x (some false) xs = anyEq x xs := by
  rw [anyLoop_acc, tvOr_false_left]

/-! ## `coalesce` -/

def coalesce : List V → V
  | [] => .null
  | .null :: rest => coalesce rest
  | v :: _ => v

/-- `coalesce` skips leading NULLs and returns the first non-NULL argument. -/
theorem coalesce_first_non_null (nulls : List V) (v : V) (rest : List V)
    (hn : ∀ n ∈ nulls, n = .null) (hv : v ≠ .null) :
    coalesce (nulls ++ v :: rest) = v := by
  induction nulls with
  | nil =>
    cases v with
    | null => exact absurd rfl hv
    | _ => rfl
  | cons n ns ih =>
    have : n = .null := hn n (by simp)
    subst this
    simp [coalesce, ih (fun m hm => hn m (by simp [hm]))]

/-! ## `WHERE` -/

def filterWhere (rows : List Row) (e : E) : List Row :=
  rows.filter fun r => truth r e == some true

/-- A row survives `WHERE` exactly when its condition is TRUE. -/
theorem filter_keeps_only_true (rows : List Row) (e : E) (r : Row) :
    r ∈ filterWhere rows e ↔ r ∈ rows ∧ truth r e = some true := by
  simp [filterWhere]

/-- An UNKNOWN condition drops the row: `WHERE NULL = 1` returns nothing. -/
theorem unknown_rows_are_dropped (rows : List Row) :
    filterWhere rows (.cmp .eq (.lit .null) (.lit (.int 1))) = [] := by
  simp [filterWhere, truth, eval, vcmp, veq, toV, toTV]

/-! ## Bounded depth (found while building W1)

  A left-deep operator chain (`x AND x AND …`, `a + 1 + 1 …`) is built by a parser
  loop, not by recursion, so the parser's nesting limit never saw it: a long chain
  produced a tree as deep as the chain was long, and every later recursive walk
  of that tree (lowering, planning, evaluation, drop) overflowed the stack. The
  fix counts each link against the same limit (`Parser::deepen`). These theorems
  show the counted loop bounds the tree: a chain the parser accepts is at most
  `maxDepth` links deeper than its operand.
-/

def depth : E → Nat
  | .col _ => 1
  | .lit _ => 1
  | .cmp _ a b => max (depth a) (depth b) + 1
  | .and a b => max (depth a) (depth b) + 1
  | .or a b => max (depth a) (depth b) + 1
  | .not a => depth a + 1
  | .isNull a => depth a + 1

/-- `x AND x AND … AND x` with `n` links, left-deep as the parser's loop builds it. -/
def andChain (x : E) : Nat → E
  | 0 => x
  | n + 1 => .and (andChain x n) x

/-- Without a bound, depth grows with the chain: one level per link. -/
theorem andChain_depth (x : E) (n : Nat) : depth (andChain x n) = depth x + n := by
  induction n with
  | zero => rfl
  | succ n ih =>
    simp only [andChain, depth, ih]
    rw [Nat.max_eq_left (Nat.le_add_right _ _)]
    omega

/-- `MAX_EXPR_DEPTH`. -/
def maxDepth : Nat := 256

/-- The parser's chain loop from counter `c`: each link checks the counter, then
    raises it (`Parser::deepen`). `none` is `ParseError::TooDeep`. -/
def deepenLoop : Nat → Nat → Option Nat
  | c, 0 => some c
  | c, n + 1 => if maxDepth ≤ c then none else deepenLoop (c + 1) n

/-- An accepted chain raised the counter by its length and never past the limit. -/
theorem deepenLoop_accepts (c n r : Nat) (h : deepenLoop c n = some r) :
    r = c + n ∧ (n = 0 ∨ c + n ≤ maxDepth) := by
  induction n generalizing c with
  | zero =>
    simp [deepenLoop] at h
    omega
  | succ n ih =>
    unfold deepenLoop at h
    split at h
    · exact absurd h (by simp)
    · have := ih (c + 1) h
      omega

/-- Every chain the parser accepts at statement level has bounded depth, so the
    recursive walks over it are bounded too. -/
theorem accepted_chain_depth_bounded (x : E) (n r : Nat) (h : deepenLoop 0 n = some r) :
    depth (andChain x n) ≤ depth x + maxDepth := by
  rw [andChain_depth]
  have := deepenLoop_accepts 0 n r h
  simp [maxDepth] at this ⊢
  omega

/-- And the loop really refuses: any chain that would pass the limit is `TooDeep`. -/
theorem deepenLoop_refuses (c n : Nat) (hn : 0 < n) (h : maxDepth < c + n) :
    deepenLoop c n = none := by
  induction n generalizing c with
  | zero => omega
  | succ n ih =>
    unfold deepenLoop
    split
    · rfl
    · exact ih (c + 1) (by simp [maxDepth] at *; omega) (by omega)

theorem deepenLoop_refuses_past_limit : deepenLoop 0 (maxDepth + 1) = none :=
  deepenLoop_refuses 0 (maxDepth + 1) (by simp [maxDepth]) (by simp [maxDepth])

end Ferrosa.Sql.Expr
