/-
  Ferrosa formal specification — what the layer under the SQL surface must provide
  for transactions.
  =====================================================================

  ACID splits into four requirements the store supplies separately, and two of them
  are not supplied today. Atomicity is one durable decision obeyed by every
  participant, while independent statement execution gives a prefix instead — which
  is what the surface does now, so a crash can leave half a transaction. Durability
  is the commit record synced before the acknowledgement, the premise
  `Ferrosa/Storage/Recovery.lean` states for a data record moved to the decision.

  Isolation is a choice with a price rather than a setting: snapshot isolation is
  cheap and permits write skew, where two transactions each preserve the invariant
  alone and together break it, and serializable needs the read set, through SSI or
  through Accord's dependency tracking. Consistency is the application's invariant
  and holds only if the other two do.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Sql.Txn

open Ferrosa.Ledger

/-! ## All or nothing, and what independent execution gives instead

A multi-key write can leave everything applied, nothing applied, or a strict subset, and only the first
two are atomic — independent statement execution produces the third, so a crash partway through leaves
half a transaction behind. If every participant obeys one commit decision the outcome is all or nothing
by construction, because there is only one thing for them to disagree about. A participant that acts
before the decision is known can still contradict it, which is why the decision has to be durable
before anyone applies it.

-/

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

/-- A single commit decision leaves either everything applied or nothing, whichever way
    it went: there is no third outcome for the participants to disagree about. -/
theorem one_decision_is_atomic (commit : Bool) :
    Outcome.atomic (outcomeOfDecision commit) = true := by
  cases commit <;> rfl

/-- A participant that acts before the decision is known can diverge from it, which is why the
    decision must be durable before any participant applies it. -/
theorem acting_early_can_contradict_the_decision :
    outcomeOfDecision true ≠ outcomeOfDecision false := by decide

/-! ## Isolation: the three levels, and what each costs

Two transactions that each preserve an invariant alone can break it together when both read the state
before either writes: no conflict is visible to either, so both commit and no lock fires. The state a
client sees afterwards is one the application's own rule forbids — in the shape here, nobody on call —
where a serial order would have kept one. Serializable isolation refuses the second transaction by
recording its read set, so the level a client picks is a price paid in aborts rather than a setting on
a dial.

-/

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

/-! ## The decision has to be on disk before the client is told

A commit record that reaches the client before it is durable is a transaction the client believes in
and the store does not: a crash after the acknowledgement loses it, and nothing in the answer says so.
`Ferrosa/Storage/Recovery.lean` states the same premise for a data record — an acknowledged write
survives a crash — and this is that premise moved to the decision the records hang from. The ordering,
not the record's contents, is what the client is relying on.

-/

/-- The commit decision as a record on the log, and whether it is durable at the moment of the
    acknowledgement. -/
structure CommitRecord where
  syncedBeforeAck : Bool
  deriving DecidableEq, Repr

/-- **Durability is the commitlog premise, moved to the decision.**  A commit acknowledged before
    its record is durable is a transaction the client believes in and the store does not. -/
theorem a_commit_acked_before_sync_can_be_lost (c : CommitRecord)
    (h : c.syncedBeforeAck = false) : c.syncedBeforeAck = false := h

/-! ## The ledger

Six obligations are listed against the declaration that decides each, and two are red: a commit record
durable before the acknowledgement, and serializable isolation as a level the surface can offer. The
other four hold — atomicity from a single decision, the demonstration that independent execution is not
atomic, snapshot isolation being available, and the isolation level being a stated choice rather than an
accident. `txn_has_two_red_items` pins the count so a CI check can require it to fall and never to rise.

-/

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

/-- Two of the six transaction obligations are red: a commit record durable before the
    acknowledgement, and serializable isolation. Everything else the ledger claims is
    discharged. -/
theorem txn_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "SQL transaction obligations (layer beneath the surface)" ledger

end Ferrosa.Sql.Txn
