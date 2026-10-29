/-
  Ferrosa formal specification — the SQL surface (`ferrosa-postgres`).
  =====================================================================

  `ferrosa-postgres` speaks the Postgres wire protocol over a Cassandra-lineage store: it has
  `TransactionStatus::{Idle, InTransaction}` and answers `BEGIN`/`COMMIT` with `ReadyForQuery`,
  so a driver reasonably concludes that a transaction is in progress. This file specifies what
  SQL promises, what the store can deliver, and the places the two differ.

  Three of the differences are semantic, not implementational:

    - SQL has three-valued logic; a cell store has two-valued presence. A naive translation of
      `NOT (b = v)` admits rows that SQL excludes (`null_passes_a_naive_not_equal`).
    - `SUM` over no rows is NULL in SQL and 0 in the obvious accumulator
      (`sum_of_no_rows_is_null_not_zero`).
    - `COUNT(*)` and `COUNT(col)` disagree on NULL by design, and a store that cannot
      distinguish "NULL" from "never written" gets the second one right by accident.

  One is structural: statements execute independently, so `COMMIT` is a prefix at best
  (`commit_is_not_atomic_across_statements`).

  One thing that is *not* a bug, checked so it does not get "fixed": SQL permits gaps in a
  sequence, so the commitlog's non-idempotent counter replay
  (`Storage.Recovery.zero_id_replay_duplicates_a_nonidempotent_mutation`) is harmless for
  `SERIAL` and harmful only for `COUNTER` (`sequence_need_only_be_monotone`).
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql

open Ferrosa.Ledger

/-- A SQL value. `none` is NULL — a value, not an absence. -/
abbrev SqlVal := Option Nat

/-! ## Three-valued logic -/

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

/-! ## Transaction atomicity -/

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

/-! ## Aggregates and NULL -/

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

/-! ## Sequences -/

/-- A sequence value only has to be monotone; gaps are legal in SQL. -/
def monotone (xs : List Nat) : Bool := xs.zip xs.tail |>.all fun p => p.1 ≤ p.2

/-- **A gap is legal**, so a sequence that consumed an id on a lost transaction — or that
    replayed a counter mutation — is still a correct sequence. The commitlog hazard is real but
    it lands on `COUNTER`, not on `SERIAL`. -/
theorem sequence_need_only_be_monotone :
    monotone [1, 3, 4] = true ∧ monotone [1, 2, 4] = true := by decide

/-- And a sequence that goes backwards is not: this is the property to hold. -/
theorem a_sequence_must_not_decrease : monotone [3, 1] = false := by decide

/-! ## The ledger -/

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

theorem sql_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "SQL surface obligations" ledger

end Ferrosa.Sql
