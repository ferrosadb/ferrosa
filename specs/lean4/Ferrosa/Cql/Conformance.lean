/-
  Ferrosa formal specification — CQL front end, part 3: the conformance corpus.
  =====================================================================

  This is the artifact that makes "the implementation agrees with the spec" a
  checkable claim rather than a hope.  It is a table:

      statement  →  (reads, writes, conditional, blind, ack plan)

  The table lives here, is checked by `decide`, and is meant to be **mirrored in
  Rust as an integration test** (`ferrosa-cql/tests/`): the same statements run
  through the real parser and planner must produce the same row.  Two directions,
  both cheap:

    * Lean → Rust: this table is the expected output.  A Rust test that disagrees
      is either a parser bug or a spec bug, and the diff says which.
    * Rust → Lean: `#eval` the Rust planner's output as a row and `decide` it
      against this table.

  What this does *not* do is prove the parser correct.  A corpus proves agreement
  on the corpus.  That is the honest boundary: the grammar is checked by
  differential testing, the semantics are proved.
-/
import Ferrosa.Cql.Classify

namespace Ferrosa.Cql

open Ferrosa

/-! ## A few statements, spelled out

These are the shapes `ferrosa-cql/src/parser.rs` produces for the corresponding
CQL text; the comment on each says which text. -/

/-- `SELECT * FROM users WHERE id = 1` -/
def qSelect : Statement :=
  .single (.select "users" [⟨"id", "=", .literal 1⟩])

/-- `INSERT INTO users (id, name) VALUES (1, 'a')` -/
def qInsert : Statement :=
  .single (.insert "users" [⟨"id", .literal 1⟩, ⟨"name", .literal 0⟩] false)

/-- `INSERT INTO users (id, name) VALUES (1, 'a') IF NOT EXISTS` -/
def qInsertIf : Statement :=
  .single (.insert "users" [⟨"id", .literal 1⟩, ⟨"name", .literal 0⟩] true)

/-- `UPDATE users SET name = 'b' WHERE id = 1 IF name = 'a'` -/
def qUpdateIf : Statement :=
  .single (.update "users" [⟨"name", .literal 0⟩] [⟨"id", "=", .literal 1⟩]
              false [⟨"name", "=", .literal 0⟩])

/-- `DELETE FROM users WHERE id = 1 IF EXISTS` -/
def qDeleteIf : Statement :=
  .single (.delete "users" [⟨"id", "=", .literal 1⟩] true [])

/-- `BEGIN BATCH INSERT …; INSERT …; APPLY BATCH` — two blind writes. -/
def qBatchBlind : Statement :=
  .batch .logged [.insert "users" [⟨"id", .literal 1⟩] false,
                  .insert "users" [⟨"id", .literal 2⟩] false]

/-- The same batch with one conditional statement in it. -/
def qBatchMixed : Statement :=
  .batch .logged [.insert "users" [⟨"id", .literal 1⟩] false,
                  .insert "users" [⟨"id", .literal 2⟩] true]

/-! ## The classification table -/

/-- `(reads, writes, conditional, blind)` for the statement. -/
def classify (s : Statement) : Bool × Bool × Bool × Bool :=
  (s.reads, s.writes, s.conditional, s.blind)

theorem qSelect_classified :
    classify qSelect = (true, false, false, false) := by decide

theorem qInsert_classified :
    classify qInsert = (false, true, false, true) := by decide

theorem qInsertIf_classified :
    classify qInsertIf = (true, true, true, false) := by decide

theorem qUpdateIf_classified :
    classify qUpdateIf = (true, true, true, false) := by decide

theorem qDeleteIf_classified :
    classify qDeleteIf = (true, true, true, false) := by decide

theorem qBatchBlind_classified :
    classify qBatchBlind = (false, true, false, true) := by decide

/-- One conditional statement is enough to make the whole batch non-blind. -/
theorem qBatchMixed_classified :
    classify qBatchMixed = (true, true, true, false) := by decide

/-! ## The ack plans for a `{3, 3}` keyspace -/

def threeThree : Strategy := .nts [("dc1", 3), ("dc2", 3)]

/-- `QUORUM` on `{3,3}`: 2 + 2, and the plan says so per DC. -/
theorem plan_quorum_3_3 :
    planFor .quorum threeThree "dc1" = ⟨4, [("dc1", 2), ("dc2", 2)]⟩ := by decide

/-- `LOCAL_QUORUM` in dc1: 2 acks, all from dc1. -/
theorem plan_local_quorum_3_3 :
    planFor .localQuorum threeThree "dc1" = ⟨2, [("dc1", 2)]⟩ := by decide

/-- `ALL` on `{3,3}`: all six. -/
theorem plan_all_3_3 :
    planFor .all threeThree "dc1" = ⟨6, []⟩ := by decide

/-- `EACH_QUORUM` on `{3,3}`: a quorum in each DC, 4 in total — the same shape
    as `QUORUM`'s per-DC plan, which is why the two are easy to conflate. -/
theorem plan_each_quorum_3_3 :
    planFor .eachQuorum threeThree "dc1" = ⟨4, [("dc1", 2), ("dc2", 2)]⟩ := by decide

/-- `RF = 1`, `ONE`: one ack. -/
theorem plan_one_rf1 : planFor .one (.simple 1) "dc1" = ⟨1, []⟩ := by decide

/-- `RF = 1`, `LOCAL_QUORUM`: still one ack, still local — the front end adds
    nothing to the topology rule. -/
theorem plan_local_quorum_rf1 : planFor .localQuorum (.simple 1) "dc1" = ⟨1, [("dc1", 1)]⟩ := by
  decide

/-- **The `{2,2}` discrepancy, in plan form.**  The specified per-DC plan needs 4;
    the implementation's global-majority arithmetic asks for 3. -/
theorem plan_quorum_2_2_differs :
    planFor .quorum (.nts [("dc1", 2), ("dc2", 2)]) "dc1" = ⟨4, [("dc1", 2), ("dc2", 2)]⟩
      ∧ blockFor .quorum 4 = 3 := by decide

/-! ## The transaction shape a `BEGIN … COMMIT` produces -/

/-- A partition key per statement: `id` for these, whatever the value.  In the
    real path this is `transaction_keys.rs` on the parsed terms. -/
def keyOf : Stmt → List Key
  | _ => [1]

theorem txn_shape_of_blind_pair :
    txnShape keyOf keyOf [.insert "users" [⟨"id", .literal 1⟩] false,
                          .insert "users" [⟨"id", .literal 2⟩] false]
      = ⟨[], [1, 1], 0⟩ := by decide

/-- Adding one conditional statement puts a key in the read set and a condition
    in the shape — the front-end half of "LWT needs a transaction". -/
theorem txn_shape_with_condition :
    txnShape keyOf keyOf [.insert "users" [⟨"id", .literal 1⟩] false,
                          .update "users" [⟨"name", .literal 0⟩]
                            [⟨"id", "=", .literal 1⟩] false [⟨"name", "=", .literal 0⟩]]
      = ⟨[1], [1, 1], 1⟩ := by decide

end Ferrosa.Cql
