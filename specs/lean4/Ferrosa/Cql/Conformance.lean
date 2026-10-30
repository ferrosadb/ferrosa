/-
  Ferrosa formal specification — CQL front end, part 3: the conformance table.
  ==========================================================================

  A handful of statements are written out in full alongside the classification
  and acknowledgement plan each produces, so that "the implementation agrees with
  the specification" is a comparison against a fixed table rather than a hope. A
  Rust integration test in `ferrosa-cql/tests/` runs the same statements through
  the real parser and planner; a row that disagrees names either a parser bug or
  a specification bug, and says which statement is at fault.

  Agreement on the table is all the table can show. It does not establish that the
  parser accepts exactly CQL — a corpus proves agreement on the corpus — and the
  grammar is left to differential testing while the semantics above the parse are
  proved in `Ferrosa/Cql/Classify.lean`.
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

/-! ## Every statement above, with the answer the classifier gives it

Each statement is pinned to a four-part answer — reads, writes, tests a condition, takes the blind path
— so a row produced by the parser can be compared with the specification without reading either. The
rows separate the cases a client notices: a plain `INSERT` is blind, an `IF` clause on an insert,
update or delete puts the statement on the read path, and a batch is blind only when every member is.
A disagreement on any row names either a parser bug or a specification bug, and the row says which
statement is at fault.

-/

/-- `(reads, writes, conditional, blind)` for the statement. -/
def classify (s : Statement) : Bool × Bool × Bool × Bool :=
  (s.reads, s.writes, s.conditional, s.blind)

/-- `SELECT * FROM users WHERE id = 1` reads without writing, tests no condition,
    and therefore takes no blind path. -/
theorem qSelect_classified :
    classify qSelect = (true, false, false, false) := by decide

/-- A plain `INSERT` writes without reading anything first, so it qualifies as a
    blind write. -/
theorem qInsert_classified :
    classify qInsert = (false, true, false, true) := by decide

/-- `INSERT … IF NOT EXISTS` reads the partition to test its condition, so it is
    not a blind write. -/
theorem qInsertIf_classified :
    classify qInsertIf = (true, true, true, false) := by decide

/-- `UPDATE … IF name = 'a'` reads, writes and tests a condition, so it needs the
    read phase. -/
theorem qUpdateIf_classified :
    classify qUpdateIf = (true, true, true, false) := by decide

/-- `DELETE … IF EXISTS` reads, writes and tests a condition, so it needs the read
    phase. -/
theorem qDeleteIf_classified :
    classify qDeleteIf = (true, true, true, false) := by decide

/-- A batch of two blind inserts is itself blind: no member reads and none tests a
    condition, so the whole batch can be acknowledged on the write path. -/
theorem qBatchBlind_classified :
    classify qBatchBlind = (false, true, false, true) := by decide

/-- One conditional statement is enough to make the whole batch non-blind. -/
theorem qBatchMixed_classified :
    classify qBatchMixed = (true, true, true, false) := by decide

/-! ## The ack plans for a `{3, 3}` keyspace

On a keyspace with three replicas in each of two datacenters, `QUORUM` asks for four acknowledgements
— two in each — `LOCAL_QUORUM` in dc1 asks for two, all in dc1, and `ALL` asks for six. `EACH_QUORUM`
produces the same plan as `QUORUM` on this shape, which is why the two are easy to conflate even
though they differ elsewhere. The last theorem here sets the plan for `{2,2}` beside the
implementation's global-majority arithmetic: the plan asks four and the code computes three.

-/

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

/-! ## The transaction shape a `BEGIN … COMMIT` produces

A transaction of blind inserts produces an empty read set and no condition, which is the shape that
can be committed on the write path alone. Adding one conditional statement puts a key in the read set
and a condition in the shape, so the same run needs a read phase before its writes can be
acknowledged — the front-end half of the argument `Ferrosa/Cql/Lwt.lean` completes. The keys are
supplied one per statement here; deriving them from the parsed terms is the real path's job and not
part of this table.

-/

/-- A partition key per statement: `id` for these, whatever the value.  In the
    real path this is `transaction_keys.rs` on the parsed terms. -/
def keyOf : Stmt → List Key
  | _ => [1]

/-- Two blind inserts between `BEGIN` and `COMMIT` give a transaction with nothing
    in its read set and no condition, which is the shape Accord can commit on the
    write path. -/
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
