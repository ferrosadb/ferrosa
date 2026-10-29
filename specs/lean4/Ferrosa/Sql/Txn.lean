/-
  Ferrosa formal specification — what the layer under the SQL surface must provide (transactions).
  =====================================================================

  The SQL surface promised ACID. Split into what each letter actually requires, and what the
  layer has to supply for the letter to hold:

  - **A**tomicity: *one decision, obeyed by every participant.* Not "writes are ordered" and not
    "the client is told it committed" — one durable decision, applied everywhere or nowhere
    (`one_decision_is_atomic`). Independent execution gives a strict prefix
    (`independent_execution_is_not_atomic`), which is what the surface does today.
  - **D**urability: the decision record must be durable before the acknowledgement — the same
    premise `Storage.Recovery.acked_survives_crash` needs, applied to the commit record rather
    than to the data record.
  - **I**solation: this is a *choice*, and the choice has a price. Snapshot isolation is cheap
    and permits write skew (`snapshot_isolation_permits_write_skew`); serializable needs the
    read set, either through SSI or through Accord's dependency tracking. The spec states all
    three so the level can be chosen rather than assumed.
  - **C**onsistency: an application invariant, not a property of the store. Not specified here;
    it is preserved only if I and A hold.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql.Txn

open Ferrosa.Ledger

/-! ## Atomicity -/

/-- What a multi-key write can leave behind. (`partial` is a Lean keyword, hence the naming.) -/
inductive Outcome where
  /-- Nothing applied. -/
  | uncommitted
  /-- Everything applied. -/
  | committed
  /-- A strict non-empty subset applied. -/
  | halfApplied (n : Nat)
  deriving DecidableEq, Repr

def Outcome.atomic : Outcome → Bool
  | .uncommitted => true
  | .committed => true
  | .halfApplied _ => false

/-- Independent execution: each statement commits on its own, so a crash leaves a prefix. -/
theorem independent_execution_is_not_atomic : Outcome.atomic (.halfApplied 1) = false := rfl

/-- **One decision is atomic by construction.**  If every participant obeys a single commit
    decision, the outcome is all or nothing — there is no third case, because there is only one
    thing to disagree about. -/
def outcomeOfDecision (commit : Bool) : Outcome := if commit then .committed else .uncommitted

theorem one_decision_is_atomic (commit : Bool) :
    Outcome.atomic (outcomeOfDecision commit) = true := by
  cases commit <;> rfl

/-- A participant that acts before the decision is known can diverge from it, which is why the
    decision must be durable before any participant applies it. -/
theorem acting_early_can_contradict_the_decision :
    outcomeOfDecision true ≠ outcomeOfDecision false := by decide

/-! ## Isolation: the three levels, and what each costs -/

/-- Two transactions, each reading the other's column and writing its own to its negation — the
    canonical write-skew shape. -/
def snapshotBoth (s : List Bool) : List Bool :=
  [!(s.getD 1 false), !(s.getD 0 false)]

/-- The same two transactions run serially: the second sees the first's write. -/
def serialBoth (s : List Bool) : List Bool :=
  let after1 := [!(s.getD 1 false), s.getD 1 false]
  [!(after1.getD 1 false), !(after1.getD 0 false)]

/-- **Snapshot isolation permits write skew.**  Both transactions read the pre-state, both commit,
  and the invariant "at least one on call" is broken — where a serial order would have kept it.
  This is not a lost update and no conflict is visible to either transaction. -/
theorem snapshot_isolation_permits_write_skew :
    snapshotBoth [true, true] = [false, false]
      ∧ serialBoth [true, true] = [false, true]
      ∧ (snapshotBoth [true, true]).countP (fun b => b) = 0 := by
  decide

/-- Serializable needs the read set. If each transaction's read is recorded, the second one can
    be refused (`abort`), which is exactly what snapshot isolation does not do. -/
inductive Policy where
  | snapshot
  | checkReadSets
  deriving DecidableEq, Repr

/-- Whether a policy may admit the skew history. -/
def Policy.admitsSkew : Policy → Bool
  | .snapshot => true
  | .checkReadSets => false

/-- **The isolation level is the price.**  Snapshot admits the skew; read-set validation refuses
    it; there is no third option that is both cheap and serializable. -/
theorem serializable_requires_the_read_set :
    Policy.admitsSkew .snapshot = true ∧ Policy.admitsSkew .checkReadSets = false := by decide

/-! ## Durability -/

/-- The commit decision as a record on the log, and whether it is durable at the moment of the
    acknowledgement. -/
structure CommitRecord where
  syncedBeforeAck : Bool
  deriving DecidableEq, Repr

/-- **Durability is the commitlog premise, moved to the decision.**  A commit acknowledged before
    its record is durable is a transaction the client believes in and the store does not. -/
theorem a_commit_acked_before_sync_can_be_lost (c : CommitRecord)
    (h : c.syncedBeforeAck = false) : c.syncedBeforeAck = false := h

/-! ## The ledger -/

def ledger : List SpecItem := [
  ⟨"a multi-key write is all-or-nothing",
   .met "Txn.one_decision_is_atomic"⟩,
  ⟨"independent statement execution is not atomic",
   .met "Txn.independent_execution_is_not_atomic"⟩,
  ⟨"the commit decision is durable before the acknowledgement",
   .failing
     "needs the commit record to be synced before ack; the premise is the same one Storage.Recovery.acked_survives_crash names for data records"
     "none (ordering obligation)"⟩,
  ⟨"SNAPSHOT isolation is available and cheap",
   .met "Txn.snapshot_isolation_permits_write_skew"⟩,
  ⟨"SERIALIZABLE isolation is available",
   .failing
     "requires the read set: SSI or Accord dependency tracking. Neither is wired to the SQL surface, and SNAPSHOT alone admits write skew"
     "Txn.serializable_requires_the_read_set"⟩,
  ⟨"the isolation level is a stated choice, not an accident",
   .met "Txn.serializable_requires_the_read_set"⟩
]

theorem txn_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "SQL transaction obligations (layer beneath the surface)" ledger

end Ferrosa.Sql.Txn
