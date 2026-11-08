/-
  Ferrosa formal specification — PL/pgSQL: the interpreter and the compiled function agree.
  ==========================================================================================

  Decision D13 (`specs/todo/postgres-frontend/decisions.md`) runs PL/pgSQL two ways. A `DO`
  block is one-shot and runs on an **interpreter** over the parsed body. `CREATE FUNCTION …
  LANGUAGE plpgsql` is **converted** to AssemblyScript and compiled to a WASM component. Both
  reach the database only through the host interface (`query`, `execute`, `notify`), and both
  evaluate every expression the way PostgreSQL does: as SQL, with the variables it mentions bound
  as parameters.

  So the two must not disagree, and this module says what agreement means and proves it for the
  subset the parser accepts (`ferrosa-plpgsql/src/ast.rs`). The interpreter keeps variables by
  **name**. The converter's real work is to give every variable a **slot** — a local in the
  generated code — and to turn each expression into SQL text plus the list of slots bound as
  `$1 … $n`. The theorem is: run the converted program on a slot store that agrees with the
  interpreter's environment, against the same host, and you get the same host calls, the same
  host state and the same returned value; and the stores still agree afterwards.

  What is not modelled: the AssemblyScript toolchain itself (the model's target language is the
  converter's output shape, not WASM), fuel and memory limits (both runs stop with an error the
  model does not need), and the text of SQL (it is an opaque string the host interprets).
-/
set_option linter.deprecated false

namespace Ferrosa.Sql.Plpgsql

/-! ## The source language: the subset the parser accepts

Values are opaque to PL/pgSQL; only the host interprets them. A value crosses the host interface
as PostgreSQL's text form with its type, so the model keeps it abstract. `none` is SQL NULL.
-/

/-- A variable name (`channel`, `notice`, `NEW.queue`, `TG_OP` — all are bound the same way). -/
abbrev Name := String

/-- A value the host returns or receives; `none` is SQL NULL. -/
abbrev Val := Option String

/-- An expression as PostgreSQL evaluates it: SQL text, and the variables it mentions, which are
    bound as `$1 … $n` in that order. -/
structure Expr where
  sql : String
  refs : List Name

/-- An `IF` condition: an ordinary boolean expression, or `[NOT] EXISTS (query)`, which is
    decided by the first row alone. -/
inductive Cond where
  | expr (e : Expr)
  | existsQ (negated : Bool) (query : Expr)

/-- The statements of the supported subset. Anything else is refused by the parser with
    `0A000`; it has no constructor here, so it cannot be "compiled to a no-op". -/
inductive Stmt where
  /-- `x := e` / `x = e`. -/
  | assign (x : Name) (e : Expr)
  /-- `PERFORM e`: evaluate and discard. -/
  | perform (e : Expr)
  /-- A SQL statement run for its effect (`CREATE TYPE …`, `INSERT …`) through `execute`. -/
  | exec (e : Expr)
  /-- `PERFORM pg_notify(c, p)`. -/
  | notify (channel payload : Expr)
  /-- `RETURN e` / `RETURN NULL` / bare `RETURN`. -/
  | ret (e : Option Expr)
  /-- `IF c THEN t ELSE f END IF` (`ELSIF` is a nested `IF` in the else branch). -/
  | ifThen (c : Cond) (t f : List Stmt)

/-! ## The host

The host is a deterministic transition over its own state σ: the calling transaction's view of
the database. `query` answers the first row's first value (or that there is no row) — the only
thing either evaluator ever reads from a result, which is also why a `query` never has to read a
second row. -/

structure Host (σ : Type) where
  /-- First value of the first row, `none` when the query returned no row. -/
  query : String → List Val → σ → Option Val × σ
  /-- Run a statement for its effect. -/
  execute : String → List Val → σ → σ
  /-- Queue a notification in the calling transaction. -/
  notify : Val → Val → σ → σ

/-- PostgreSQL's reading of a condition value: only `true` is true; NULL and `false` are not. -/
def truth : Val → Bool
  | some "t" => true
  | _ => false

/-- The value an expression evaluates to: the first column of `SELECT expr`'s one row (NULL if,
    impossibly, no row came back). -/
def scalar : Option Val → Val
  | some v => v
  | none => none

/-- How a run ends: it fell off the end of the body, or returned. -/
inductive Outcome (σ : Type) where
  | done (s : σ)
  | returned (v : Val) (s : σ)

/-! ## The interpreter (the `DO` path)

The environment maps names to values. Statement lists run left to right and stop at `RETURN`.
-/

section Interp
variable {σ : Type} (h : Host σ)

/-- Bind an expression's variables from the environment, in order. -/
def bindNames (env : Name → Val) (e : Expr) : List Val := e.refs.map env

/-- An expression's value: `SELECT e` through the host — except an expression that *is* one
    variable (`$1` bound to it), which is that variable's value, exactly what `SELECT $1`
    returns. Both runtimes take this shortcut, so it is part of the semantics. -/
def evalE (env : Name → Val) (s : σ) (e : Expr) : Val × σ :=
  match e.refs with
  | [x] => if e.sql = "$1" then (env x, s) else
      let (r, s') := h.query e.sql (bindNames env e) s
      (scalar r, s')
  | _ =>
      let (r, s') := h.query e.sql (bindNames env e) s
      (scalar r, s')

def evalCond (env : Name → Val) (s : σ) : Cond → Bool × σ
  | .expr e =>
      let (v, s') := evalE h env s e
      (truth v, s')
  | .existsQ neg q =>
      let (r, s') := h.query q.sql (bindNames env q) s
      (if neg then r.isNone else r.isSome, s')

mutual
/-- Run one statement. -/
def interp (env : Name → Val) (s : σ) : Stmt → (Name → Val) × Outcome σ
  | .assign x e =>
      let (v, s') := evalE h env s e
      (fun y => if y = x then v else env y, .done s')
  | .perform e => (env, .done (h.query e.sql (bindNames env e) s).2)
  | .exec e => (env, .done (h.execute e.sql (bindNames env e) s))
  | .notify c p =>
      let (vc, s1) := evalE h env s c
      let (vp, s2) := evalE h env s1 p
      (env, .done (h.notify vc vp s2))
  | .ret none => (env, .returned none s)
  | .ret (some e) =>
      let (v, s') := evalE h env s e
      (env, .returned v s')
  | .ifThen c t f =>
      let (b, s') := evalCond h env s c
      if b then interpList env s' t else interpList env s' f

/-- Run a statement list, stopping at the first `RETURN`. -/
def interpList (env : Name → Val) (s : σ) : List Stmt → (Name → Val) × Outcome σ
  | [] => (env, .done s)
  | st :: rest =>
      match interp env s st with
      | (env', .done s') => interpList env' s' rest
      | (env', .returned v s') => (env', .returned v s')
end

end Interp

/-! ## The converter and the compiled program (the `CREATE FUNCTION` path)

The converter assigns each variable a slot — a local in the generated AssemblyScript — and lowers
every expression to its SQL text plus the slots bound as parameters. The compiled program runs on
a slot store. -/

/-- A lowered expression: SQL text and the slots bound as `$1 … $n`. -/
structure CExpr where
  sql : String
  slots : List Nat

inductive CCond where
  | expr (e : CExpr)
  | existsQ (negated : Bool) (query : CExpr)

inductive CStmt where
  | assign (slot : Nat) (e : CExpr)
  | perform (e : CExpr)
  | exec (e : CExpr)
  | notify (channel payload : CExpr)
  | ret (e : Option CExpr)
  | ifThen (c : CCond) (t f : List CStmt)

section Convert
variable (layout : Name → Nat)

def lowerExpr (e : Expr) : CExpr := ⟨e.sql, e.refs.map layout⟩

@[simp] theorem lowerExpr_sql (e : Expr) : (lowerExpr layout e).sql = e.sql := rfl

def lowerCond : Cond → CCond
  | .expr e => .expr (lowerExpr layout e)
  | .existsQ n q => .existsQ n (lowerExpr layout q)

mutual
/-- The converter: total on the subset, one target statement per source statement. -/
def convert : Stmt → CStmt
  | .assign x e => .assign (layout x) (lowerExpr layout e)
  | .perform e => .perform (lowerExpr layout e)
  | .exec e => .exec (lowerExpr layout e)
  | .notify c p => .notify (lowerExpr layout c) (lowerExpr layout p)
  | .ret none => .ret none
  | .ret (some e) => .ret (some (lowerExpr layout e))
  | .ifThen c t f => .ifThen (lowerCond layout c) (convertList t) (convertList f)

def convertList : List Stmt → List CStmt
  | [] => []
  | st :: rest => convert st :: convertList rest
end

end Convert

section Run
variable {σ : Type} (h : Host σ)

def bindSlots (store : Nat → Val) (e : CExpr) : List Val := e.slots.map store

/-- The generated code's expression evaluation, with the same one-variable shortcut. -/
def runE (store : Nat → Val) (s : σ) (e : CExpr) : Val × σ :=
  match e.slots with
  | [k] => if e.sql = "$1" then (store k, s) else
      let (r, s') := h.query e.sql (bindSlots store e) s
      (scalar r, s')
  | _ =>
      let (r, s') := h.query e.sql (bindSlots store e) s
      (scalar r, s')

def runCond (store : Nat → Val) (s : σ) : CCond → Bool × σ
  | .expr e =>
      let (v, s') := runE h store s e
      (truth v, s')
  | .existsQ neg q =>
      let (r, s') := h.query q.sql (bindSlots store q) s
      (if neg then r.isNone else r.isSome, s')

mutual
/-- The compiled program's semantics: what the generated code does, statement by statement. -/
def run (store : Nat → Val) (s : σ) : CStmt → (Nat → Val) × Outcome σ
  | .assign k e =>
      let (v, s') := runE h store s e
      (fun j => if j = k then v else store j, .done s')
  | .perform e => (store, .done (h.query e.sql (bindSlots store e) s).2)
  | .exec e => (store, .done (h.execute e.sql (bindSlots store e) s))
  | .notify c p =>
      let (vc, s1) := runE h store s c
      let (vp, s2) := runE h store s1 p
      (store, .done (h.notify vc vp s2))
  | .ret none => (store, .returned none s)
  | .ret (some e) =>
      let (v, s') := runE h store s e
      (store, .returned v s')
  | .ifThen c t f =>
      let (b, s') := runCond h store s c
      if b then runList store s' t else runList store s' f

def runList (store : Nat → Val) (s : σ) : List CStmt → (Nat → Val) × Outcome σ
  | [] => (store, .done s)
  | st :: rest =>
      match run store s st with
      | (store', .done s') => runList store' s' rest
      | (store', .returned v s') => (store', .returned v s')
end

end Run

/-! ## Agreement

A slot store *agrees* with an environment under a layout when every variable's slot holds that
variable's value. The layout must be injective — two variables never share a local — or an
assignment to one would clobber the other. -/

def Agrees (layout : Name → Nat) (env : Name → Val) (store : Nat → Val) : Prop :=
  ∀ x, store (layout x) = env x

def Injective (layout : Name → Nat) : Prop := ∀ x y, layout x = layout y → x = y

/-- Binding by slots, under agreement, binds exactly what binding by names binds: the same
    parameters reach the host. -/
theorem binds_agree (layout : Name → Nat) (env : Name → Val) (store : Nat → Val)
    (hag : Agrees layout env store) (e : Expr) :
    bindSlots store (lowerExpr layout e) = bindNames env e := by
  unfold bindSlots bindNames lowerExpr
  simp only [List.map_map]
  apply List.map_congr_left
  intro x _
  exact hag x

/-- Evaluation agrees: the one-variable shortcut reads the same value from the slot as from
    the name, and every other expression sends the same SQL with the same parameters. -/
theorem eval_agrees {σ : Type} (h : Host σ) (layout : Name → Nat) (env : Name → Val)
    (store : Nat → Val) (hag : Agrees layout env store) (s : σ) (e : Expr) :
    runE h store s (lowerExpr layout e) = evalE h env s e := by
  have hb := binds_agree layout env store hag e
  unfold runE evalE
  simp only [lowerExpr] at hb ⊢
  cases hr : e.refs with
  | nil => simp only [hr, List.map_nil] at hb ⊢; rw [hb]
  | cons x rest =>
    cases rest with
    | nil => simp only [hr, List.map_cons, List.map_nil, hag x] at hb ⊢; rw [hb]
    | cons y more => simp only [hr, List.map_cons] at hb ⊢; rw [hb]

/-- Assigning the same value to `x` and to its slot preserves agreement (this is where
    injectivity is needed). -/
theorem assign_agrees (layout : Name → Nat) (hinj : Injective layout)
    (env : Name → Val) (store : Nat → Val) (hag : Agrees layout env store) (x : Name) (v : Val) :
    Agrees layout (fun y => if y = x then v else env y)
      (fun j => if j = layout x then v else store j) := by
  intro y
  by_cases hy : y = x
  · subst hy; simp
  · have : layout y ≠ layout x := fun e => hy (hinj y x e)
    simp [hy, this, hag y]

theorem cond_agrees {σ : Type} (h : Host σ) (layout : Name → Nat) (env : Name → Val)
    (store : Nat → Val) (hag : Agrees layout env store) (s : σ) (c : Cond) :
    runCond h store s (lowerCond layout c) = evalCond h env s c := by
  cases c with
  | expr e =>
      simp only [runCond, evalCond, lowerCond, eval_agrees h layout env store hag s e]
  | existsQ n q =>
      simp only [runCond, evalCond, lowerCond, lowerExpr_sql,
        binds_agree layout env store hag q]

/-- What "the two runs agree" means for one pair of results: the same outcome (same host state,
    same returned value) and stores that still agree. -/
def ResultAgrees {σ : Type} (layout : Name → Nat) :
    ((Name → Val) × Outcome σ) → ((Nat → Val) × Outcome σ) → Prop
  | (env, .done s), (store, .done s') => s = s' ∧ Agrees layout env store
  | (env, .returned v s), (store, .returned v' s') => v = v' ∧ s = s' ∧ Agrees layout env store
  | _, _ => False

mutual
/-- **The converter's output, run, equals the interpreter's result** — statement form. -/
theorem convert_agrees {σ : Type} (h : Host σ) (layout : Name → Nat) (hinj : Injective layout) :
    ∀ (st : Stmt) (env : Name → Val) (store : Nat → Val) (s : σ),
      Agrees layout env store →
      ResultAgrees layout (interp h env s st) (run h store s (convert layout st))
  | .assign x e, env, store, s, hag => by
      simp only [interp, run, convert, eval_agrees h layout env store hag, ResultAgrees, true_and]
      exact assign_agrees layout hinj env store hag x _
  | .perform e, env, store, s, hag => by
      simp only [interp, run, convert, lowerExpr_sql, binds_agree layout env store hag e, ResultAgrees, true_and]
      exact hag
  | .exec e, env, store, s, hag => by
      simp only [interp, run, convert, lowerExpr_sql, binds_agree layout env store hag e, ResultAgrees, true_and]
      exact hag
  | .notify c p, env, store, s, hag => by
      simp only [interp, run, convert, eval_agrees h layout env store hag, ResultAgrees, true_and]
      exact hag
  | .ret none, env, store, s, hag => by
      simp only [interp, run, convert, ResultAgrees, true_and]
      exact hag
  | .ret (some e), env, store, s, hag => by
      simp only [interp, run, convert, eval_agrees h layout env store hag, ResultAgrees, true_and]
      exact hag
  | .ifThen c t f, env, store, s, hag => by
      simp only [interp, run, convert, cond_agrees h layout env store hag s c]
      split
      · exact convertList_agrees h layout hinj t env store _ hag
      · exact convertList_agrees h layout hinj f env store _ hag

/-- **The converter's output, run, equals the interpreter's result** — statement-list form, the
    shape of a whole function body. -/
theorem convertList_agrees {σ : Type} (h : Host σ) (layout : Name → Nat) (hinj : Injective layout) :
    ∀ (body : List Stmt) (env : Name → Val) (store : Nat → Val) (s : σ),
      Agrees layout env store →
      ResultAgrees layout (interpList h env s body) (runList h store s (convertList layout body))
  | [], env, store, s, hag => by
      simp only [interpList, runList, convertList, ResultAgrees, true_and]
      exact hag
  | st :: rest, env, store, s, hag => by
      have hst := convert_agrees h layout hinj st env store s hag
      simp only [interpList, runList, convertList]
      revert hst
      cases interp h env s st with
      | mk env' o =>
        cases run h store s (convert layout st) with
        | mk store' o' =>
          cases o <;> cases o' <;> simp only [ResultAgrees] <;> intro hst
          · obtain ⟨hs, hag'⟩ := hst
            subst hs
            exact convertList_agrees h layout hinj rest env' store' _ hag'
          · exact hst.elim
          · exact hst.elim
          · obtain ⟨hv, hs, hag'⟩ := hst
            exact ⟨hv, hs, hag'⟩
end

/-- The corollary a reader wants: started from agreeing stores (the arguments bound into their
    slots), a function body returns the same value and leaves the host — the calling
    transaction — in the same state, whichever way it ran. -/
theorem compiled_returns_what_the_interpreter_returns {σ : Type} (h : Host σ)
    (layout : Name → Nat) (hinj : Injective layout) (body : List Stmt)
    (env : Name → Val) (store : Nat → Val) (s : σ) (hag : Agrees layout env store)
    (v : Val) (s' : σ) (env' : Name → Val)
    (hi : interpList h env s body = (env', .returned v s')) :
    ∃ store', runList h store s (convertList layout body) = (store', .returned v s') := by
  have hr := convertList_agrees h layout hinj body env store s hag
  rw [hi] at hr
  revert hr
  cases runList h store s (convertList layout body) with
  | mk store' o =>
    cases o with
    | done _ => intro hr; exact hr.elim
    | returned v' s'' =>
      intro hr
      obtain ⟨hv, hs, _⟩ := hr
      subst hv; subst hs
      exact ⟨store', rfl⟩

/-! ## `EXISTS` reads at most one row

`evalCond` on `.exists` consults only whether the first row is present, so the host may stop the
query after one row: a second row can change nothing. -/

theorem exists_ignores_everything_after_the_first_row {σ : Type} (h₁ h₂ : Host σ)
    (env : Name → Val) (s : σ) (n : Bool) (q : Expr)
    (hfirst : ∀ sql ps s, ((h₁.query sql ps s).1.isSome = (h₂.query sql ps s).1.isSome) ∧
      (h₁.query sql ps s).2 = (h₂.query sql ps s).2) :
    evalCond h₁ env s (.existsQ n q) = evalCond h₂ env s (.existsQ n q) := by
  simp only [evalCond]
  obtain ⟨h1, h2⟩ := hfirst q.sql (bindNames env q) s
  cases n
  · simp only [Bool.false_eq_true, ite_false]; rw [h1, h2]
  · simp only [ite_true]
    cases e1 : (h₁.query q.sql (bindNames env q) s).1 <;>
      cases e2 : (h₂.query q.sql (bindNames env q) s).1 <;> simp_all

/-! ## The census capture: flattening a statement onto one line

`ferrosa-postgres/tests/fixtures/oban_census.sql` stores one statement per line, so the capture
replaced newlines with spaces. That is harmless for SQL except inside a `--` comment, which runs
to the end of the *line*: flattened, it swallows the code that followed it. Oban's v1 notify
function has such a comment, and its flattened body no longer parses (the census test in
`ferrosa-plpgsql/tests/census.rs` found it). A `/* … */` comment ends at its delimiter, so it
survives flattening; the fixture now uses that form, which PostgreSQL reads identically. -/

/-- Replace each newline with a space, as the capture did. -/
def flatten (s : List Char) : List Char := s.map (fun c => if c = '\n' then ' ' else c)

mutual
/-- Strip `--` comments (to the end of the line), keeping the newline. -/
def lineCode : List Char → List Char
  | '-' :: '-' :: rest => lineComment rest
  | c :: rest => c :: lineCode rest
  | [] => []

def lineComment : List Char → List Char
  | '\n' :: rest => '\n' :: lineCode rest
  | _ :: rest => lineComment rest
  | [] => []
end

mutual
/-- Strip `/* … */` comments (unnested, enough for the census), leaving a space. -/
def blockCode : List Char → List Char
  | '/' :: '*' :: rest => ' ' :: blockComment rest
  | c :: rest => c :: blockCode rest
  | [] => []

def blockComment : List Char → List Char
  | '*' :: '/' :: rest => blockCode rest
  | _ :: rest => blockComment rest
  | [] => []
end

/-- **Flattening a line comment swallows the code after it**: the code a parser sees is not the
    flattened form of the code the author wrote. -/
theorem flattening_a_line_comment_swallows_code :
    lineCode (flatten "x -- note\ny".toList) ≠ flatten (lineCode "x -- note\ny".toList) := by
  decide

/-- **A block comment survives flattening**: stripping it before or after flattening gives the
    same code. -/
theorem a_block_comment_survives_flattening :
    blockCode (flatten "x /* note */\ny".toList) = flatten (blockCode "x /* note */\ny".toList) := by
  decide

end Ferrosa.Sql.Plpgsql
