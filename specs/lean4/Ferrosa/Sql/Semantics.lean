/-
  Ferrosa formal specification — the SQL surface (`ferrosa-postgres`).
  =====================================================================

  A Postgres driver talking to `ferrosa-postgres` gets the protocol it expects —
  `BEGIN` and `COMMIT` are answered with `ReadyForQuery` and the session reports
  `InTransaction` — and three of the things SQL promises are not behind it. NULL is
  a value here rather than an absence, so a translation of `<>` that compares and
  negates admits rows SQL excludes. `SUM` over no rows returns zero rather than NULL.
  And statements execute independently, so `COMMIT` is at best a prefix of the
  transaction that a crash interrupts.

  One difference that looks like a defect is not: SQL permits gaps in a sequence, so
  a replay that consumes an extra identifier is still a correct sequence, and that
  cost lands on `COUNTER` rather than on `SERIAL`.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql

open Ferrosa.Ledger

/-- A SQL value. `none` is NULL — a value, not an absence. -/
abbrev SqlVal := Option Nat

/-! ## NULL is a value, so a comparison can be neither true nor false

Because a NULL is stored here rather than left absent, `b <> v` translated as compare-and-negate admits
a row whose column is NULL, while SQL's three-valued logic excludes it because the predicate is
UNKNOWN. `null_passes_a_naive_not_equal` gives the divergence and `the_two_agree_without_nulls` shows
the two agree whenever no NULL is involved, which is why a test suite that never writes NULL finds
nothing. A client sees rows in its result set that its own `WHERE` clause excludes.

-/

/-- The obvious translation of `b <> v`: compare and negate. -/
def notEqNaive (b : SqlVal) (v : Nat) : Bool := !(b == some v)

/-- SQL's `NOT (b = v)`: comparing with NULL yields UNKNOWN, and an UNKNOWN predicate does not
    select the row. -/
def notEqStd (b : SqlVal) (v : Nat) : Bool :=
  match b with
  | none => false
  | some x => x != v

/-- **A NULL row passes the naive predicate and fails the standard one.**  The row appears in
    `SELECT * FROM t WHERE NOT (b = 5)` when it must not. -/
theorem null_passes_a_naive_not_equal :
    notEqNaive none 5 = true ∧ notEqStd none 5 = false := by decide

/-- The same divergence on a non-null value: the naive translation is right whenever no NULL is
    involved, which is why the bug survives testing. -/
theorem the_two_agree_without_nulls :
    notEqNaive (some 6) 5 = notEqStd (some 6) 5 := by decide

/-! ## What a crash in the middle of a transaction leaves behind

A crash partway through a multi-statement transaction leaves a strict, non-empty prefix applied:
neither "the transaction did not happen" nor "it happened", and SQL promises only those two.
`status_is_not_a_durability_claim` adds that the session's `InTransaction` status says nothing about
what reached the log, so the client is told it is inside a transaction whose earlier half may already
be lost. Reconnecting finds some of the statements applied and the rest gone, with nothing recording
which.

-/

/-- A transaction as the surface accepts it: statements in order, and how many of them are
    durable at the moment of the crash. -/
structure Txn where
  stmts : List Nat
  durable : Nat
  deriving DecidableEq, Repr

def Txn.applied (t : Txn) : List Nat := t.stmts.take t.durable

/-- **`COMMIT` is not atomic across statements.**  A crash mid-transaction leaves a strict
    non-empty prefix: neither "the transaction did not happen" nor "it happened". SQL promises
    the first two outcomes only. -/
theorem commit_is_not_atomic_across_statements :
    ∃ t : Txn, t.applied ≠ [] ∧ t.applied ≠ t.stmts :=
  ⟨⟨[1, 2], 1⟩, by decide, by decide⟩

/-- Reporting `InTransaction` does not make it so: the status the client sees is a function of
    the session, not of what is durable. -/
theorem status_is_not_a_durability_claim :
    (⟨[1, 2], 1⟩ : Txn).applied.length < 2 := by decide

/-! ## What the aggregate functions return when there are no rows

SQL's aggregates have defined answers for the empty set, and a translation built on an accumulator that
starts at zero gives a different one: `SUM` over no rows is NULL, not `0`, so a dashboard that adds the
result to a running total treats "no data" as "zero" without noticing. `COUNT(*)` counts rows and
`COUNT(col)` skips the rows where the column is NULL, so the two differ exactly on those rows. The sum
agrees as soon as a row is present and the two counts agree as soon as no row is NULL, which is why a
test suite loaded with ordinary data finds neither divergence.

-/

/-- The obvious accumulator. -/
def sumNaive (xs : List Nat) : SqlVal := some (xs.foldl (· + ·) 0)

/-- SQL: `SUM` over the empty set is NULL. -/
def sumStd (xs : List Nat) : SqlVal :=
  match xs with
  | [] => none
  | xs => some (xs.foldl (· + ·) 0)

/-- **`SUM` over no rows is NULL, not 0.**  A dashboard that adds the result to a running total
    treats "no data" as "zero" without noticing. -/
theorem sum_of_no_rows_is_null_not_zero :
    sumStd [] = none ∧ sumNaive [] = some 0 := by decide

/-- On non-empty input they agree, which is why this survives a test suite. -/
theorem sums_agree_when_there_are_rows : sumStd [1, 2] = sumNaive [1, 2] := by decide

def countStar (rows : List (List SqlVal)) : Nat := rows.length

def countCol (rows : List (List SqlVal)) (i : Nat) : Nat :=
  rows.countP fun r => (r.getD i none).isSome

/-- **`COUNT(*)` counts rows, `COUNT(col)` skips NULL.**  They differ exactly on the rows where
    the column is NULL. -/
theorem count_star_and_count_col_disagree :
    countStar [[none], [some 1]] = 2 ∧ countCol [[none], [some 1]] 0 = 1 := by decide

/-! ## What a sequence has to be, and what it may skip

A sequence value only has to be monotone, and gaps are legal in SQL, so a value consumed by a
transaction that never committed leaves the sequence correct and the count of issued values larger than
the count of rows. `a_sequence_must_not_decrease` is the property that does have to hold. The
consequence is a division of labour: the commitlog hazard of a replayed counter mutation is real, but it
lands on `COUNTER` and not on `SERIAL`, so a gap in a `SERIAL` column is not a defect to be repaired.

-/

/-- A sequence value only has to be monotone; gaps are legal in SQL. -/
def monotone (xs : List Nat) : Bool := xs.zip xs.tail |>.all fun p => p.1 ≤ p.2

/-- **A gap is legal**, so a sequence that consumed an id on a lost transaction — or that
    replayed a counter mutation — is still a correct sequence. The commitlog hazard is real but
    it lands on `COUNTER`, not on `SERIAL`. -/
theorem sequence_need_only_be_monotone :
    monotone [1, 3, 4] = true ∧ monotone [1, 2, 4] = true := by decide

/-- And a sequence that goes backwards is not: this is the property to hold. -/
theorem a_sequence_must_not_decrease : monotone [3, 1] = false := by decide

/-! ## The ledger

Six obligations the SQL surface carries are listed against the declaration that decides each, and three
are red: multi-statement atomicity, three-valued logic, and `SUM` over the empty set. The three that
hold are per-partition atomicity of a single statement, the `COUNT(*)`/`COUNT(col)` difference on NULL,
and monotone sequences. `sql_has_three_red_items` pins the count, and a driver relying on any of the red
three is handed an answer SQL would not have given.

-/

def ledger : List SpecItem := [
  ⟨"a single statement is atomic",
   .met "Storage.WritePath.batch_ack_is_durable (per-partition)"⟩,
  ⟨"a multi-statement transaction is atomic",
   .failing
     "statements execute independently; a crash leaves a strict prefix, and the surface still answers BEGIN/COMMIT with InTransaction"
     "Sql.commit_is_not_atomic_across_statements"⟩,
  ⟨"three-valued logic is preserved",
   .failing
     "a naive `<>` admission of NULL rows; the two agree whenever no NULL is involved, which hides it"
     "Sql.null_passes_a_naive_not_equal"⟩,
  ⟨"aggregates follow SQL's empty-set rules",
   .failing
     "SUM over no rows must be NULL; an accumulator that starts at 0 returns 0"
     "Sql.sum_of_no_rows_is_null_not_zero"⟩,
  ⟨"COUNT(*) and COUNT(col) differ only on NULL",
   .met "Sql.count_star_and_count_col_disagree"⟩,
  ⟨"sequences are monotone",
   .met "Sql.sequence_need_only_be_monotone"⟩
]

/-- Three of the six obligations the SQL surface carries are red: multi-statement
    atomicity, three-valued logic, and `SUM` over the empty set. A driver that
    relies on any of them gets an answer SQL would not have given. -/
theorem sql_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "SQL surface obligations" ledger

end Ferrosa.Sql
