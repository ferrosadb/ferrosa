/-
  Ferrosa formal specification — general `UPDATE` / `DELETE` (lane W2).
  ====================================================================

  Oban claims, acknowledges and prunes jobs with `UPDATE … FROM … WHERE … RETURNING`,
  `UPDATE … WHERE id = $1 AND state = ANY($2)` and `DELETE … WHERE id IN (SELECT … LIMIT n)`.
  The engine has no in-place update operator. It runs such a statement in two steps:

  1. **Read.** One general `SELECT` over the target, joined with the `FROM` list and the
     CTEs, under the statement's snapshot with the transaction's own writes overlaid. It
     projects each matching target row's storage key, the new value of every `SET` column,
     and the `RETURNING` values computed over the new row.
  2. **Write.** Each projected row becomes one keyed write through the ordinary write path.

  This file pins why that is the declarative statement and not an approximation of it:

  * `read_then_write_eq_declarative` / `delete_read_then_write_eq_declarative`: with unique
    storage keys, keyed writes of the matching rows give exactly the declarative result.
  * `returning_sees_new_row`: `RETURNING` reads a target column through a remapped slot that
    holds the `SET` value. That is evaluation over the new row.
  * `firstPerRun_strict`, `firstPerRun_complete`, `firstPerRun_first`: an `UPDATE … FROM`
    join can match one target row several times. The rows are stably sorted by key, and only
    the first row of each run of equal keys is kept. So each target row is written once
    (keys strictly increase), every matched row is written, and the kept row is the first
    match in arrival order. PostgreSQL also applies only one match.
  * `legacy_where_agrees`, `legacy_set_agrees`: the legacy keyed `UpdateStmt` that the parser
    still produces for pgbench's shapes means what the general form means. Its equality
    conjunction is TRUE exactly when the general three-valued AND chain is TRUE, and its
    `SET` arithmetic is the general arithmetic.
  * `explicit_value_wins`, `omitted_column_takes_default`: `INSERT` fills an omitted column
    from its DEFAULT, or NULL when it has none. A written value always wins.
  * `rc_no_double_claim` (decision D16): under READ COMMITTED a writer takes ownership of
    the row, then re-reads it. If the row changed since the snapshot the statement fails
    with 40001 rather than re-evaluating the predicate. So two claimers of one row can never
    both write it.

  Source: `ferrosa-sql/src/general/dml.rs` (the read), `ferrosa-postgres/src/dml_general.rs`
  (the write).
-/
import Ferrosa.Sql.Expr

namespace Ferrosa.Sql.Dml

open Ferrosa.Sql.Expr

/-! ## 1. Read-then-write is the declarative statement

A general `UPDATE` or `DELETE` is run as one read that plans a keyed write per matching row,
followed by those keyed writes. This section shows that, when storage keys are unique, the two
steps produce exactly the table the declarative statement describes, for `UPDATE` and for
`DELETE`.
-/

section ReadThenWrite

variable {K R : Type} [DecidableEq K]

/-- Look a key up in the write plan: the first entry with that key. -/
def find (k : K) : List (K × R) → Option R
  | [] => none
  | (k', v) :: rest => if k' = k then some v else find k rest

/-- The read: each matching row's key with its new image. -/
def plan (key : R → K) (p : R → Bool) (f : R → R) (t : List R) : List (K × R) :=
  (t.filter p).map (fun r => (key r, f r))

/-- The write: each row whose key the plan names takes its planned image. -/
def applyPlan (key : R → K) (w : List (K × R)) (t : List R) : List R :=
  t.map (fun r => (find (key r) w).getD r)

/-- The declarative `UPDATE t SET … WHERE p`. -/
def declarative (p : R → Bool) (f : R → R) (t : List R) : List R :=
  t.map (fun r => if p r then f r else r)

/-- A key that no row of the table carries is absent from the write plan: the read never invents
    a write for a row that does not exist. -/
theorem find_none_of_not_mem (key : R → K) (p : R → Bool) (f : R → R) (k : K) :
    ∀ t : List R, k ∉ t.map key → find k (plan key p f t) = none
  | [] => fun _ => rfl
  | x :: xs => by
    intro h
    have hx : key x ≠ k := fun e => h (by simp [e])
    have hxs : k ∉ xs.map key := fun m => h (List.mem_cons_of_mem _ m)
    have ih := find_none_of_not_mem key p f k xs hxs
    by_cases px : p x = true
    · simp [plan, List.filter, px, find, hx] at ih ⊢
      exact ih
    · simp [plan, List.filter, px] at ih ⊢
      exact ih

/-- With unique keys, looking a row's key up in the plan gives its new image exactly when the
    row matches the predicate, and nothing otherwise. This is the step that makes one keyed
    write per planned row the same as updating the row in place. -/
theorem find_plan (key : R → K) (p : R → Bool) (f : R → R) :
    ∀ t : List R, (t.map key).Nodup → ∀ r ∈ t,
      find (key r) (plan key p f t) = if p r then some (f r) else none
  | [] => by intro _ r hr; cases hr
  | x :: xs => by
    intro hnd r hr
    have hnd' : key x ∉ xs.map key ∧ (xs.map key).Nodup := by
      simpa [List.map, List.nodup_cons] using hnd
    cases hr with
    | head =>
      by_cases px : p x = true
      · simp [plan, List.filter, px, find]
      · have hnone := find_none_of_not_mem key p f (key x) xs hnd'.1
        simp [plan, List.filter, px] at hnone ⊢
        exact hnone
    | tail _ hmem =>
      have hne : key x ≠ key r := by
        intro e
        exact hnd'.1 (e ▸ List.mem_map_of_mem hmem)
      have ih := find_plan key p f xs hnd'.2 r hmem
      by_cases px : p x = true
      · simp [plan, List.filter, px, find, hne] at ih ⊢
        exact ih
      · simp [plan, List.filter, px] at ih ⊢
        exact ih

/-- Keyed writes of the matching rows are the declarative update, when keys are unique. -/
theorem read_then_write_eq_declarative (key : R → K) (p : R → Bool) (f : R → R)
    (t : List R) (hnd : (t.map key).Nodup) :
    applyPlan key (plan key p f t) t = declarative p f t := by
  unfold applyPlan declarative
  apply List.map_congr_left
  intro r hr
  rw [find_plan key p f t hnd r hr]
  by_cases pr : p r = true <;> simp [pr]

/-- The read of a `DELETE`: the matching rows' keys. -/
def deletePlan (key : R → K) (p : R → Bool) (t : List R) : List K :=
  (t.filter p).map key

/-- Keyed tombstones: drop every row whose key the plan names. -/
def applyDeletes (key : R → K) (ks : List K) (t : List R) : List R :=
  t.filter (fun r => !(decide (key r ∈ ks)))

omit [DecidableEq K] in
/-- With unique keys, a row's key is in the `DELETE` plan exactly when the row matches the
    predicate, so a keyed tombstone removes that row and no other. -/
theorem mem_deletePlan (key : R → K) (p : R → Bool) :
    ∀ t : List R, (t.map key).Nodup → ∀ r ∈ t, (key r ∈ deletePlan key p t ↔ p r = true)
  | [] => by intro _ r hr; cases hr
  | x :: xs => by
    intro hnd r hr
    have hnd' : key x ∉ xs.map key ∧ (xs.map key).Nodup := by
      simpa [List.map, List.nodup_cons] using hnd
    have hsub : ∀ k, k ∈ deletePlan key p xs → k ∈ xs.map key := by
      intro k hk
      simp only [deletePlan, List.mem_map, List.mem_filter] at hk
      obtain ⟨y, ⟨hy, _⟩, rfl⟩ := hk
      exact List.mem_map_of_mem hy
    cases hr with
    | head =>
      by_cases px : p x = true
      · simp [deletePlan, List.filter, px]
      · have : key x ∉ deletePlan key p xs := fun m => hnd'.1 (hsub _ m)
        simp [deletePlan, List.filter, px] at this ⊢
        exact this
    | tail _ hmem =>
      have hne : key x ≠ key r := by
        intro e
        exact hnd'.1 (e ▸ List.mem_map_of_mem hmem)
      have ih := mem_deletePlan key p xs hnd'.2 r hmem
      by_cases px : p x = true
      · simp only [deletePlan, List.filter, px, List.map_cons, List.mem_cons]
        constructor
        · intro h
          cases h with
          | inl e => exact absurd e.symm hne
          | inr m => exact ih.mp m
        · intro h; exact Or.inr (ih.mpr h)
      · simp [deletePlan, List.filter, px] at ih ⊢
        exact ih

/-- Keyed tombstones of the matching rows are the declarative `DELETE`. -/
theorem delete_read_then_write_eq_declarative (key : R → K) (p : R → Bool)
    (t : List R) (hnd : (t.map key).Nodup) :
    applyDeletes key (deletePlan key p t) t = t.filter (fun r => !p r) := by
  unfold applyDeletes
  apply List.filter_congr
  intro r hr
  have h := mem_deletePlan key p t hnd r hr
  by_cases pr : p r = true
  · simp [pr, h.mpr pr]
  · have : key r ∉ deletePlan key p t := fun m => pr (h.mp m)
    simp [pr, this]

end ReadThenWrite

/-! ## 2. `RETURNING` is evaluated over the new row

The read's row is `joined ++ setValues`. A reference to an assigned target column is
compiled to the slot of its `SET` value (`slot`), every other reference to its own slot.
-/

/-- A small expression language over a row given as a function. -/
inductive X where
  | col (i : Nat)
  | lit (v : V)
  | add (a b : X)
  | eq (a b : X)
  deriving Repr

def addV : V → V → V
  | .int a, .int b => .int (a + b)
  | _, _ => .null

def ev (env : Nat → V) : X → V
  | .col i => env i
  | .lit v => v
  | .add a b => addV (ev env a) (ev env b)
  | .eq a b => toV (veq (ev env a) (ev env b))

def remap (slot : Nat → Nat) : X → X
  | .col i => .col (slot i)
  | .lit v => .lit v
  | .add a b => .add (remap slot a) (remap slot b)
  | .eq a b => .eq (remap slot a) (remap slot b)

/-- Every column an expression reads lies below `w`. -/
def Scoped (w : Nat) : X → Prop
  | .col i => i < w
  | .lit _ => True
  | .add a b => Scoped w a ∧ Scoped w b
  | .eq a b => Scoped w a ∧ Scoped w b

/-- Evaluating an expression whose column references were renumbered through `slot` is
    evaluating the original expression over the row read through `slot`. This is how a compiled
    `RETURNING` expression is related back to the one the client wrote. -/
theorem ev_remap (env : Nat → V) (slot : Nat → Nat) :
    ∀ e, ev env (remap slot e) = ev (fun i => env (slot i)) e
  | .col _ => rfl
  | .lit _ => rfl
  | .add a b => by simp [ev, remap, ev_remap env slot a, ev_remap env slot b]
  | .eq a b => by simp [ev, remap, ev_remap env slot a, ev_remap env slot b]

/-- Two rows that agree on every column below `w` give the same value for any expression that
    reads only columns below `w`: what an expression does not read cannot change its result. -/
theorem ev_congr (f g : Nat → V) (w : Nat) (h : ∀ i, i < w → f i = g i) :
    ∀ e, Scoped w e → ev f e = ev g e
  | .col i, hs => h i hs
  | .lit _, _ => rfl
  | .add a b, hs => by simp [ev, ev_congr f g w h a hs.1, ev_congr f g w h b hs.2]
  | .eq a b, hs => by simp [ev, ev_congr f g w h a hs.1, ev_congr f g w h b hs.2]

/-- The read's row: `joined` (width `J`) followed by the `SET` values. -/
def ext (J : Nat) (joined : Nat → V) (vals : Nat → V) (i : Nat) : V :=
  if i < J then joined i else vals (i - J)

/-- Target columns are `[0, w)`; `asg i = some k` when column `i` is the `k`-th SET. -/
def slot (w J : Nat) (asg : Nat → Option Nat) (i : Nat) : Nat :=
  if i < w then (match asg i with | some k => J + k | none => i) else i

/-- The joined row after the update: assigned target columns hold their SET values. -/
def newRow (w : Nat) (asg : Nat → Option Nat) (joined vals : Nat → V) (i : Nat) : V :=
  if i < w then (match asg i with | some k => vals k | none => joined i) else joined i

/-- Reading the extended row through the slot map gives the new row's value in every joined
    column: an assigned target column yields its `SET` value, and any other column yields
    itself. -/
theorem slot_reads_new (w J : Nat) (asg : Nat → Option Nat)
    (joined vals : Nat → V) (i : Nat) (hi : i < J) :
    ext J joined vals (slot w J asg i) = newRow w asg joined vals i := by
  unfold slot newRow ext
  by_cases h : i < w
  · simp only [h, ite_true]
    cases asg i with
    | none => simp [hi]
    | some k =>
      have hk : ¬ (J + k < J) := by omega
      simp [hk]
  · simp [h, hi]

/-- `RETURNING e`, compiled through the slot map, is `e` evaluated over the new row. -/
theorem returning_sees_new_row (w J : Nat) (asg : Nat → Option Nat)
    (joined vals : Nat → V) (e : X) (hs : Scoped J e) :
    ev (ext J joined vals) (remap (slot w J asg) e) = ev (newRow w asg joined vals) e := by
  rw [ev_remap]
  exact ev_congr _ _ J (fun i hi => slot_reads_new w J asg joined vals i hi) e hs

/-! ## 3. `UPDATE … FROM`: each target row is written once, by its first match

A join in `UPDATE … FROM` can match one target row several times. The planned rows are stably
sorted by key, and `firstPerRun` keeps the first row of each run of equal keys. The theorems
below show that no key is written twice, that every matched key is written, and that the row
kept is the first match in arrival order.
-/

/-- Keep the first row of each run of equal keys; `prev` is the key of the last kept row. -/
def firstPerRun {α : Type} : Option Nat → List (Nat × α) → List (Nat × α)
  | _, [] => []
  | prev, (k, v) :: rest =>
    if prev = some k then firstPerRun prev rest else (k, v) :: firstPerRun (some k) rest

/-- Sorted by key, ascending (the stable sort's output). -/
def Sorted {α : Type} (l : List (Nat × α)) : Prop := l.Pairwise (fun a b => a.1 ≤ b.1)

/-- Every key of `l` is at least `prev`. -/
def Above {α : Type} (prev : Option Nat) (l : List (Nat × α)) : Prop :=
  ∀ p ∈ l, ∀ k, prev = some k → k ≤ p.1

/-- Every row `firstPerRun` keeps has a key strictly greater than the previously kept key, given
    sorted input. This is the induction step behind `firstPerRun_strict`. -/
theorem firstPerRun_above {α : Type} :
    ∀ (l : List (Nat × α)) (prev : Option Nat), Sorted l → Above prev l →
      ∀ q ∈ firstPerRun prev l, ∀ k, prev = some k → k < q.1
  | [], _, _, _ => by intro q hq; cases hq
  | (k0, v0) :: rest, prev, hs, ha => by
    have hs' : (∀ b ∈ rest, k0 ≤ b.1) ∧ Sorted rest := by
      simpa [Sorted, List.pairwise_cons] using hs
    intro q hq k hk
    unfold firstPerRun at hq
    by_cases e : prev = some k0
    · rw [ite_eq_left e] at hq
      have ha' : Above prev rest := fun p hp => ha p (List.mem_cons_of_mem _ hp)
      exact firstPerRun_above rest prev hs'.2 ha' q hq k hk
    · rw [ite_eq_right e] at hq
      have hle : k ≤ k0 := ha (k0, v0) (List.mem_cons_self) k hk
      have hne : k ≠ k0 := fun h => e (h ▸ hk)
      cases hq with
      | head => show k < k0; omega
      | tail _ hm =>
        have ha' : Above (some k0) rest := by
          intro p hp k' hk'
          cases hk'
          exact hs'.1 p hp
        have := firstPerRun_above rest (some k0) hs'.2 ha' q hm k0 rfl
        omega

/-- Kept keys strictly increase: no target row is written twice. -/
theorem firstPerRun_strict {α : Type} :
    ∀ (l : List (Nat × α)) (prev : Option Nat), Sorted l → Above prev l →
      (firstPerRun prev l).Pairwise (fun a b => a.1 < b.1)
  | [], _, _, _ => List.Pairwise.nil
  | (k0, v0) :: rest, prev, hs, ha => by
    have hs' : (∀ b ∈ rest, k0 ≤ b.1) ∧ Sorted rest := by
      simpa [Sorted, List.pairwise_cons] using hs
    unfold firstPerRun
    by_cases e : prev = some k0
    · rw [ite_eq_left e]
      exact firstPerRun_strict rest prev hs'.2 (fun p hp => ha p (List.mem_cons_of_mem _ hp))
    · rw [ite_eq_right e]
      have ha' : Above (some k0) rest := by
        intro p hp k' hk'
        cases hk'
        exact hs'.1 p hp
      refine List.Pairwise.cons ?_ (firstPerRun_strict rest (some k0) hs'.2 ha')
      intro q hq
      exact firstPerRun_above rest (some k0) hs'.2 ha' q hq k0 rfl

/-- Every matched key is written (or is the key already written just before). -/
theorem firstPerRun_complete {α : Type} :
    ∀ (l : List (Nat × α)) (prev : Option Nat), ∀ p ∈ l,
      (∃ q ∈ firstPerRun prev l, q.1 = p.1) ∨ prev = some p.1
  | [], _ => by intro p hp; cases hp
  | (k0, v0) :: rest, prev => by
    intro p hp
    unfold firstPerRun
    by_cases e : prev = some k0
    · rw [ite_eq_left e]
      cases hp with
      | head => exact Or.inr e
      | tail _ hm => exact firstPerRun_complete rest prev p hm
    · rw [ite_eq_right e]
      cases hp with
      | head => exact Or.inl ⟨(k0, v0), List.mem_cons_self, rfl⟩
      | tail _ hm =>
        cases firstPerRun_complete rest (some k0) p hm with
        | inl h =>
          obtain ⟨q, hq, hqe⟩ := h
          exact Or.inl ⟨q, List.mem_cons_of_mem _ hq, hqe⟩
        | inr h =>
          exact Or.inl ⟨(k0, v0), List.mem_cons_self, Option.some.inj h⟩

/-- A kept row is the first row with its key: every row before it has another key. -/
theorem firstPerRun_first {α : Type} :
    ∀ (l : List (Nat × α)) (prev : Option Nat), Sorted l → Above prev l →
      ∀ q ∈ firstPerRun prev l, ∃ pre suf, l = pre ++ q :: suf ∧ ∀ p ∈ pre, p.1 ≠ q.1
  | [], _, _, _ => by intro q hq; cases hq
  | (k0, v0) :: rest, prev, hs, ha => by
    have hs' : (∀ b ∈ rest, k0 ≤ b.1) ∧ Sorted rest := by
      simpa [Sorted, List.pairwise_cons] using hs
    have ha0 : Above prev rest := fun p hp => ha p (List.mem_cons_of_mem _ hp)
    intro q hq
    unfold firstPerRun at hq
    by_cases e : prev = some k0
    · rw [ite_eq_left e] at hq
      obtain ⟨pre, suf, hl, hpre⟩ := firstPerRun_first rest prev hs'.2 ha0 q hq
      have hlt := firstPerRun_above rest prev hs'.2 ha0 q hq k0 e
      refine ⟨(k0, v0) :: pre, suf, by simp [hl], ?_⟩
      intro p hp
      cases hp with
      | head => show k0 ≠ q.1; omega
      | tail _ hm => exact hpre p hm
    · rw [ite_eq_right e] at hq
      have ha' : Above (some k0) rest := by
        intro p hp k' hk'
        cases hk'
        exact hs'.1 p hp
      cases hq with
      | head => exact ⟨[], rest, rfl, by intro p hp; cases hp⟩
      | tail _ hm =>
        obtain ⟨pre, suf, hl, hpre⟩ := firstPerRun_first rest (some k0) hs'.2 ha' q hm
        have hlt := firstPerRun_above rest (some k0) hs'.2 ha' q hm k0 rfl
        refine ⟨(k0, v0) :: pre, suf, by simp [hl], ?_⟩
        intro p hp
        cases hp with
        | head => show k0 ≠ q.1; omega
        | tail _ hm' => exact hpre p hm'

/-! ## 4. The legacy keyed statement means what the general one means

The parser still produces the legacy keyed `UpdateStmt` for pgbench's statement shapes. This
section shows that its equality conjunction and its `SET` arithmetic mean what the general
three-valued form means, so the two code paths cannot disagree on which rows match or on what is
written.
-/

/-- The legacy keyed `WHERE c = v AND …` (`predicate_matches`): NULL matches nothing. -/
def legacyWhere (r : Row) : List (Nat × V) → Bool
  | [] => true
  | (i, v) :: rest => (v != .null && colOf r i == v) && legacyWhere r rest

/-- The same conjunction as a general three-valued `AND` chain. -/
def andChain : List (Nat × V) → E
  | [] => .lit (.bool true)
  | (i, v) :: rest => .and (.cmp .eq (.col i) (.lit v)) (andChain rest)

/-- A three-valued `AND` is TRUE exactly when both sides are TRUE. A NULL side never makes the
    conjunction TRUE. -/
theorem tvAnd_true_iff (a b : TV) : tvAnd a b = some true ↔ a = some true ∧ b = some true := by
  cases a with
  | none => cases b with
    | none => simp [tvAnd]
    | some y => cases y <;> simp [tvAnd]
  | some x => cases x <;> cases b with
    | none => simp [tvAnd]
    | some y => cases y <;> simp [tvAnd]

/-- Three-valued equality is TRUE exactly when the right side is not NULL and the two values are
    equal. This is the legacy keyed predicate's rule, which treats a NULL comparand as matching
    nothing. -/
theorem veq_true_iff (x v : V) : veq x v = some true ↔ (v != .null && x == v) = true := by
  cases x <;> cases v <;> simp [veq]

/-- The legacy conjunction is TRUE exactly when the general AND chain is TRUE. -/
theorem legacy_where_agrees (r : Row) :
    ∀ eqs : List (Nat × V), truth r (andChain eqs) = some true ↔ legacyWhere r eqs = true
  | [] => by simp [truth, andChain, eval, toTV, legacyWhere]
  | (i, v) :: rest => by
    have ih := legacy_where_agrees r rest
    simp only [truth] at ih
    simp only [truth, andChain, eval, vcmp, toTV_toV, tvAnd_true_iff, legacyWhere,
      Bool.and_eq_true]
    rw [ih, veq_true_iff]
    simp [colOf]

/-- The legacy `SET` right-hand side (`SetExpr`): a constant, a column, or `a + b`. -/
inductive SetE where
  | val (v : V)
  | col (i : Nat)
  | add (a b : SetE)

/-- The legacy evaluator (`eval_set_expr` / `apply_arith`): NULL propagates. -/
def legacySet (r : Nat → V) : SetE → V
  | .val v => v
  | .col i => r i
  | .add a b => addV (legacySet r a) (legacySet r b)

/-- The general expression the same text parses to. -/
def lowerSet : SetE → X
  | .val v => .lit v
  | .col i => .col i
  | .add a b => .add (lowerSet a) (lowerSet b)

/-- The legacy `SET` evaluator and the general expression evaluator give the same value for
    every right-hand side the legacy parser accepts, NULL propagation included. So pgbench's
    keyed `UPDATE` writes what the general `UPDATE` would. -/
theorem legacy_set_agrees (r : Nat → V) : ∀ e, legacySet r e = ev r (lowerSet e)
  | .val _ => rfl
  | .col _ => rfl
  | .add a b => by simp [legacySet, ev, lowerSet, legacy_set_agrees r a, legacy_set_agrees r b]

/-! ## 5. `INSERT` fills omitted columns from their defaults

`INSERT (cols) VALUES (vals)` stores a written value where one is given, the column's DEFAULT
where it is omitted and a default exists, and NULL otherwise. Oban's job inserts rely on all
three cases.
-/

/-- The value `INSERT (cols) VALUES (vals)` stores in column `i`. -/
def fill (cols : List Nat) (vals : Nat → V) (dflt : Nat → Option V) (i : Nat) : V :=
  if i ∈ cols then vals i else (dflt i).getD .null

/-- A column named in `INSERT (cols)` stores the value written for it, whatever its DEFAULT
    says. -/
theorem explicit_value_wins (cols : List Nat) (vals : Nat → V) (dflt : Nat → Option V)
    (i : Nat) (h : i ∈ cols) : fill cols vals dflt i = vals i := by
  simp [fill, h]

/-- A column left out of `INSERT (cols)` that has a DEFAULT stores that default. -/
theorem omitted_column_takes_default (cols : List Nat) (vals : Nat → V)
    (dflt : Nat → Option V) (i : Nat) (h : i ∉ cols) (d : V) (hd : dflt i = some d) :
    fill cols vals dflt i = d := by
  simp [fill, h, hd]

/-- A column left out of `INSERT (cols)` that has no DEFAULT stores NULL, as in PostgreSQL. The
    column is neither refused nor left unwritten. -/
theorem omitted_without_default_is_null (cols : List Nat) (vals : Nat → V)
    (dflt : Nat → Option V) (i : Nat) (h : i ∉ cols) (hd : dflt i = none) :
    fill cols vals dflt i = .null := by
  simp [fill, h, hd]

/-! ## 5b. Assignment conversion never turns a value into NULL

A `SET col = <text>` into a jsonb (or timestamp) column parses the text. Parsing either
yields a value of the column's type or fails the statement (22P02). The bug this pins:
the cast used to be refused outright; the rule is that it can never quietly yield NULL.
-/

/-- Convert a text value (`none` is SQL NULL) to a column type with `parse`. -/
def assignText {J : Type} (parse : String → Option J) : Option String → Except Unit (Option J)
  | none => .ok none
  | some s => match parse s with
    | some j => .ok (some j)
    | none => .error ()

/-- Assigning SQL NULL to a jsonb or timestamp column stores NULL; there is no text to parse. -/
theorem assign_null_is_null {J : Type} (parse : String → Option J) :
    assignText parse none = .ok none := rfl

/-- Assigning a non-NULL text value never stores NULL: the text either parses to a value of the
    column's type or the statement fails. This pins the bug where a refused cast could otherwise
    have degraded to a silent NULL. -/
theorem assign_never_nulls {J : Type} (parse : String → Option J) (s : String) :
    assignText parse (some s) ≠ .ok none := by
  simp only [assignText]
  match h : parse s with
  | none => simp
  | some j => simp

/-! ## 5c. `RETURNING` reports the row as stored

A `timestamp` cell keeps whole milliseconds: the store maps microseconds `m` to
`m / 1000 * 1000` (toward zero). Oban's ack compares the stored column with the value
`RETURNING` gave it (`attempted_at = $5`). If `RETURNING` reports the unstored value, the
two differ whenever the value had sub-millisecond digits, and the ack matches nothing
(`unstored_returning_can_miss`). Reporting `store v` matches, because storing is
idempotent (`returning_reports_the_stored_value`).
-/

/-- The millisecond store of a microsecond timestamp (Rust's `/` truncates toward zero). -/
def storeMillis (m : Int) : Int := m.tdiv 1000 * 1000

/-- Storing a timestamp that is already in whole milliseconds leaves it unchanged: the
    millisecond store is idempotent. -/
theorem storeMillis_idem (m : Int) : storeMillis (storeMillis m) = storeMillis m := by
  unfold storeMillis
  rw [Int.mul_tdiv_cancel _ (by decide : (1000 : Int) ≠ 0)]

/-- The stored cell equals what `RETURNING` reported, once that is the stored value. -/
theorem returning_reports_the_stored_value (m : Int) :
    storeMillis (storeMillis m) = storeMillis m :=
  storeMillis_idem m

/-- Reporting the unstored value can miss: a microsecond value is not its stored form. -/
theorem unstored_returning_can_miss : storeMillis 1500 ≠ 1500 := by decide

/-! ## 6. READ COMMITTED: re-check after ownership, fail on a concurrent change (D16)

Under READ COMMITTED a writer first takes ownership of the row and then re-reads it. If the row
has changed since the statement's snapshot, the write fails with 40001 instead of re-evaluating
the predicate (decision D16). The theorems below show that an unchanged row is written as
declared, and that two claimers of one row can never both write it.
-/

/-- Write `new` over a row read as `snap`, finding it now as `cur`: 40001 if it moved. -/
def rcWrite (snap cur new : V) : Except Unit V :=
  if cur = snap then .ok new else .error ()

/-- Unchanged since the snapshot: the write is the declarative one. -/
theorem rc_unchanged_writes (snap new : V) : rcWrite snap snap new = .ok new := by
  simp [rcWrite]

/-- Two claimers read the row as `s`. The first changes it to `a ≠ s`. The second, which
owns the row only after the first commits, finds `a` and fails: never a double claim. -/
theorem rc_no_double_claim (s a b : V) (h : a ≠ s) : rcWrite s a b = .error () := by
  simp [rcWrite, h]

/-! ## 7. A rewritten call keeps its output name

The front end rewrites `obj_description(oid, cat)` into a `pg_description` subquery.
A select item's default output name comes from its expression: a function call is named
after the function, a subquery `?column?`. So a rewrite that does not pin the name
renames the client's column (`rewrite_without_alias_renames`). Pinning the original
name as the alias keeps it (`rewrite_with_alias_keeps_name`).
-/

inductive Item where
  | func (name : String)
  | subquery

/-- PostgreSQL's default output name. -/
def defaultName : Item → String
  | .func n => n
  | .subquery => "?column?"

/-- The output name of an item with an optional alias. -/
def outName (alias : Option String) (i : Item) : String := alias.getD (defaultName i)

/-- Without an alias, the rewritten subquery is named `?column?` and the original call was named
    `obj_description`, so a rewrite that does not pin the name renames the client's column. -/
theorem rewrite_without_alias_renames :
    outName none .subquery ≠ outName none (.func "obj_description") := by decide

/-- Aliasing the rewritten subquery with the original function's name gives it exactly the
    output name the original call had, so the client sees the column it asked for. -/
theorem rewrite_with_alias_keeps_name (n : String) :
    outName (some (defaultName (.func n))) .subquery = outName none (.func n) := rfl

end Ferrosa.Sql.Dml
