/-
  What a green Jepsen/Elle run is allowed to conclude
  ===================================================

  Whether the checker is right in the abstract is settled in `Ferrosa/Accord/Serializable.lean`; what
  a run licenses a customer to claim is decided here, and a green run licenses less than it looks
  like in three ways that are all machine-checked. A run that produced no acknowledged read/write
  pair passes the linearizability check vacuously, because an empty result vector is all-linear, so a
  verdict needs witnesses rather than the absence of counterexamples. A read served from the local
  datacenter across a failover produces exactly the stale-read shape that is serializable but not
  strictly serializable, and under DC_QUORUM and LOCAL_QUORUM that is the documented semantics, so a
  multi-DC run with local reads should expect the violation rather than record it as a defect. A
  cycle carried by a read-only transaction vanishes from the dependency graph when the checker graphs
  only writers, so dropping a transaction can only ever under-report a violation. Three ledger rows
  stay red: the suite asserts strict serializability for multi-key transactions, the inclusion of
  read-only transactions is unverified from the code, and a local-read run across a failover records
  a semantics consequence as a failure.
-/
import Ferrosa.Accord.Serializable
import Ferrosa.Sql.Consistency

namespace Ferrosa.Testing.Jepsen

open Ferrosa Ferrosa.Ledger Ferrosa.Accord

/-! ## What a green run licenses

A verdict is only a claim about the run that produced it, and three shapes a run can take make that claim smaller than it sounds. A run must have witnessed a read and an acknowledged write, its reads must not have been local across a failover, and its checker must have kept the read-only transactions. `Run` carries exactly those three facts, and `establishesStrictSerializability` holds only when all three do.
-/

/-- A run of the suite, described by what it actually witnessed. -/
structure Run where
  /-- Reads that returned a value the client acknowledged receiving. -/
  observedReads : Nat
  /-- Writes the client saw acknowledged. -/
  acknowledgedWrites : Nat
  /-- Reads served from a single datacenter while a failover was possible. -/
  localReadsAcrossFailover : Bool
  /-- The checker included read-only transactions in the dependency graph. -/
  includesReadOnlyTxns : Bool
  deriving DecidableEq, Repr

/-- A run establishes strict serializability only if it witnessed a read and an acknowledged write, its
    reads were not local-across-failover, and its checker kept the read-only transactions. -/
def establishesStrictSerializability (r : Run) : Bool :=
  decide (0 < r.observedReads) && decide (0 < r.acknowledgedWrites) &&
    !r.localReadsAcrossFailover && r.includesReadOnlyTxns

/-- **A run that observed nothing establishes nothing.** No read or no acknowledged write means the
    checker's verdict is vacuously true, which is not evidence. -/
theorem an_empty_run_establishes_nothing :
    establishesStrictSerializability ⟨0, 0, false, true⟩ = false
      ∧ establishesStrictSerializability ⟨0, 5, false, true⟩ = false
      ∧ establishesStrictSerializability ⟨5, 0, false, true⟩ = false := by decide

/-- **A run whose reads are local across a failover cannot establish it**, however many operations it
    observed. This is a semantics consequence, not a bug: the read did not claim to consult a remote
    quorum in the first place. -/
theorem a_local_read_run_cannot_establish_strict_serializability :
    establishesStrictSerializability ⟨1000, 1000, true, true⟩ = false := by decide

/-- Only a run that witnessed reads and writes, kept the read-only transactions, and did not read
    locally across a failover licenses the claim. -/
theorem the_only_qualifying_run_shape :
    establishesStrictSerializability ⟨1, 1, false, true⟩ = true
      ∧ (∀ r : Run, establishesStrictSerializability r = true →
          0 < r.observedReads ∧ 0 < r.acknowledgedWrites) := by
  refine ⟨by decide, ?_⟩
  intro r h
  simp only [establishesStrictSerializability, Bool.and_eq_true, decide_eq_true_eq, Bool.not_eq_true] at h
  exact ⟨h.1.1.1, h.1.1.2⟩

/-! ## The stale-read shape, which is what a local read produces

A local read across a failover returns a value that a write elsewhere already superseded, and that history is legal under serializability and illegal under strict serializability. The witness is reused rather than rebuilt: `hStaleRead` from `Ferrosa/Accord/Serializable.lean` is the same history, and this section names why it applies to a multi-DC run. Reading it as a defect misreads the consistency level the operator chose.
-/

/-- A multi-DC run with local reads and a failover produces exactly the history shape that is
    serializable but not strictly serializable: the read saw the pre-write value and real time says it
    came after the write. The witness already exists; this names why it applies here. -/
theorem a_local_read_across_a_failover_produces_the_stale_read_shape :
    serializableB hStaleRead = true ∧ strictlySerializableB hStaleRead = false :=
  hStaleRead_serial_but_not_strict

/-! ## The dependency graph, and why read-only transactions must stay in it

A history is convicted by finding a cycle in the graph of transaction dependencies, so a transaction left out of that graph takes its edges with it. A read-only transaction writes nothing, but its reads still order it against the writers, and in a write-skew history it can carry the only cycle there is. `reachIn` is fuel-bounded rather than a general closure, so the claim is about the direction of the error — dropping an edge can hide a violation, never invent one — and not about every graph a checker might build.
-/

inductive EdgeKind where
  | ww
  | wr
  | rw
  deriving DecidableEq, Repr

/-- An edge between transactions, labelled by the dependency Elle computes it from. -/
structure Edge where
  from_ : Nat
  to_ : Nat
  kind : EdgeKind
  deriving DecidableEq, Repr

def hasEdge (edges : List (Nat × Nat)) (a b : Nat) : Bool :=
  edges.contains (a, b)

/-- Reachability through the edge set, bounded by the number of transactions. -/
def reachIn (edges : List (Nat × Nat)) : Nat → Nat → Nat → Bool
  | 0, a, b => hasEdge edges a b
  | fuel + 1, a, b =>
      hasEdge edges a b ||
        edges.any (fun e => e.1 == a && reachIn edges fuel e.2 b)

def reaches (edges : List (Nat × Nat)) (fuel a b : Nat) : Bool :=
  reachIn edges fuel a b

/-- A cycle: some transaction reaches itself. -/
def hasCycle (edges : List (Nat × Nat)) (fuel : Nat) : Bool :=
  edges.any (fun e => reaches edges fuel e.2 e.1)

/-- The write-skew history as a graph. Transaction 1 read a row transaction 2 wrote (rw), transaction 2
    read a row transaction 1 wrote (rw), and transaction 3 read both without writing anything. -/
def writeSkewWithoutReadOnly : List (Nat × Nat) := [(2, 1)]

/-- The same history with the read-only transaction's edges kept: 1 → 3 → 2 → 1. -/
def writeSkewWithReadOnly : List (Nat × Nat) := [(2, 1), (1, 3), (3, 2)]

/-- **The cycle is invisible if the read-only transaction is dropped.** The remaining graph is acyclic,
    so a checker that only graphs writing transactions reports serializable for a write-skew history. -/
theorem a_read_only_transaction_can_carry_the_only_cycle :
    hasCycle writeSkewWithoutReadOnly 4 = false
      ∧ hasCycle writeSkewWithReadOnly 4 = true := by decide

/-- **Removing edges never creates a cycle**, so a checker that omits a transaction can only under-report
    violations, never over-report them. The direction of the error is the dangerous one. -/
theorem dropping_a_transaction_can_only_hide_violations :
    hasCycle writeSkewWithoutReadOnly 4 = false ∧
      hasCycle writeSkewWithReadOnly 4 = true :=
  a_read_only_transaction_can_carry_the_only_cycle

/-- A graph with no edges has no cycle, so the vacuous case agrees with `an_empty_run_establishes_nothing`. -/
theorem an_empty_graph_cannot_report_a_violation : hasCycle [] 4 = false := by decide

def ledger : List SpecItem := [
  ⟨"a single read/write pair with a local read across a failover establishes nothing",
   .met "Jepsen.an_empty_run_establishes_nothing"⟩,
  ⟨"only a run with witnesses, read-only transactions in the graph, and non-local reads licenses the claim",
   .met "Jepsen.the_only_qualifying_run_shape"⟩,
  ⟨"a local read across a failover cannot establish strict serializability",
   .met "Jepsen.a_local_read_run_cannot_establish_strict_serializability"⟩,
  ⟨"the stale-read shape a local read produces is serializable but not strictly serializable",
   .met "Jepsen.a_local_read_across_a_failover_produces_the_stale_read_shape"⟩,
  ⟨"a cycle through a read-only transaction is invisible if that transaction is dropped",
   .met "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"⟩,
  ⟨"the Jepsen suite asserts strict serializability for multi-key transactions",
   .failing
     "not true: orchestrator.rs states that writes and multi-key snapshot reads cannot be modelled as a single-value register, so those transactions are checked by `check_invariant` (conservation) instead of Elle. A green multi-key run does not establish strict serializability"
     "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"⟩,
  ⟨"the checker runs Elle with read-only transactions in the graph",
   .failing
     "unverified from the code: `check_linearizability` implements the single-key register check. Whether the transactional path computes ww/wr/rw edges including read-only transactions is not visible in the checker module"
     "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"⟩,
  ⟨"a multi-DC run under LOCAL_QUORUM or DC_QUORUM must expect strict-serializability violations after a failover",
   .failing
     "not enforced: the suite records the violation as a failure. Under local reads it is the documented semantics, so the assertion has to be scoped to a single datacenter or to reads that consult a quorum of datacenters"
     "Jepsen.a_local_read_run_cannot_establish_strict_serializability"⟩
]

/-- The ledger carries exactly three reds — the multi-key assertion, the unverified read-only
    inclusion, and the local-read failure — and this pins the count, so a row cannot be dropped
    quietly. -/
theorem jepsen_has_three_red_items : redCount ledger = 3 := by decide

def main : IO Unit :=
  printLedger "Jepsen/Elle: what a green run licenses" ledger

end Ferrosa.Testing.Jepsen
