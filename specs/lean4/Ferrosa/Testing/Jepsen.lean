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
  only writers, so dropping a transaction can only ever under-report a violation. A checker that did
  not run must fail the run rather than count as a pass, and a verdict must be the checker's exit
  code rather than a search of its output. Four ledger rows are red. No transactional checker runs on
  a suite run, so the suite neither asserts strict serializability for multi-key transactions nor
  depends on a checker that keeps read-only transactions; and the Rust suite both counts an absent
  checker as a pass and reads Elle's verdict by searching text. A local-read run across a failover is
  classified as not tested, not failed.
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

/-! ## A checker that did not run, and a verdict read from text

A run is green only if every checker it needed said so. Two ways to get that wrong are modelled here, because both are present in the Rust suite. The first treats a checker that produced no result as a pass, so a run whose transactional checker never started is indistinguishable from one it cleared. The second reads the verdict by searching the checker's output for `:valid? true`, which a nested result inside an invalid verdict also contains. The specification's rules are the opposite of both: a required checker that did not run fails the run, and the verdict is the checker's exit code. These are decisions D2 and D3 of `specs/jepsen-evidence/decisions.md` in the suite repository.
-/

/-- A checker outcome: `none` when it did not run, `some v` when it ran and returned `v`. -/
abbrev Outcome := Option Bool

/-- The rule the Rust suite uses for an optional checker: one that did not run counts as a pass. -/
def passesTreatingAbsentAsPass (results : List Outcome) : Bool :=
  results.all (fun r => r.getD true)

/-- The specification's rule for a checker the workload requires: it must have run and passed. -/
def passesRequiringEveryChecker (results : List Outcome) : Bool :=
  results.all (fun r => r == some true)

/-- **A required checker that did not run must fail the run.** Treating an absent result as a pass
    turns "the transactional checker never started" into a green run, which is the shape of a suite
    run today; requiring the result makes it red. -/
theorem an_absent_required_checker_must_fail_the_run :
    passesTreatingAbsentAsPass [some true, none] = true
      ∧ passesRequiringEveryChecker [some true, none] = false := by decide

/-- The two rules agree whenever every checker ran, so requiring the result costs nothing on a run
    that produced one. -/
theorem the_rules_agree_when_every_checker_ran (results : List Bool) :
    passesTreatingAbsentAsPass (results.map some)
      = passesRequiringEveryChecker (results.map some) := by
  induction results with
  | nil => rfl
  | cons b bs ih =>
      cases b <;> simp_all [passesTreatingAbsentAsPass, passesRequiringEveryChecker]

/-- A verdict read by searching the output for a marker, as the Rust `ElleChecker` does. -/
def scrapedValid (output : String) : Bool := (output.splitOn ":valid? true").length > 1

/-- **A verdict searched for in the output passes an invalid result.** Elle reports per-checker
    sub-results, so an invalid history's output can carry `:valid? true` inside a nested map. -/
theorem a_scraped_verdict_passes_an_invalid_result :
    scrapedValid "{:valid? false, :anomaly-types (:G-single), :sub {:valid? true}}" = true := by
  native_decide

/-- **And a case-folded search never finds an anomaly.** Elle names anomalies in upper case
    (`:G-single`), so searching for the lower-cased keyword reports none. -/
theorem a_lower_cased_anomaly_search_finds_nothing :
    ("{:valid? false, :anomaly-types (:G-single)}".splitOn ":g-single").length = 1 := by
  native_decide

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
     "not true: no transactional checker runs on a CQL run. `UnifiedChecker::check_all` leaves Elle unset, and `checker::strict_serializable` is called only by the Postgres integration test, so multi-key workloads such as `bank` are judged by `check_invariant` (conservation) alone. A green multi-key run does not establish strict serializability. Wiring a checker is not enough on its own: a SELECT inside a CQL transaction reads committed state outside Accord and COMMIT discards the transaction's reads, so no client can observe a multi-key read-only transaction, which is the shape this row's witness needs (specs/jepsen-evidence D5, packet JE-9)"
     "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"⟩,
  ⟨"the checker runs Elle with read-only transactions in the graph",
   .failing
     "not run: the in-process checker `checker::strict_serializable` keeps read-only transactions, and its test `a_read_only_transaction_can_carry_the_only_cycle` rejects this file's history; but neither it nor Elle is invoked on a suite run, so no run's verdict depends on it"
     "Jepsen.a_read_only_transaction_can_carry_the_only_cycle"⟩,
  ⟨"a multi-DC run under LOCAL_QUORUM or DC_QUORUM must expect strict-serializability violations after a failover",
   .met "Jepsen.a_local_read_run_cannot_establish_strict_serializability"⟩,
  ⟨"a checker the workload requires that did not run fails the run",
   .failing
     "not true: the suite's verdict is linearizability and the workload's invariant, and nothing a workload declares makes a transactional checker required, so a multi-key run whose transactional checker never ran is green. `AllCheckResults::all_passed` also counts an absent Elle result as a pass, though no suite run reaches it (specs/jepsen-evidence D2, packet JE-1)"
     "Jepsen.an_absent_required_checker_must_fail_the_run"⟩,
  ⟨"the Elle verdict is the checker's exit code, not a search of its output",
   .failing
     "not true: `ElleChecker::run_check` decides validity by searching stdout for `:valid? true` and looks for anomaly names in lower case, so a nested sub-result passes an invalid history and no anomaly type is ever reported. The code is not reached on a suite run today, which is the previous row (specs/jepsen-evidence D3, packet JE-2)"
     "Jepsen.a_scraped_verdict_passes_an_invalid_result"⟩
]

/-- The ledger carries four reds: the multi-key assertion and the read-only inclusion, because no
    transactional checker runs on a suite run; an absent checker counted as a pass; and a verdict
    read by searching text. This pins the count, so a row cannot be dropped quietly. The local-read
    row is met: `checker::profile` classifies a declared cross-datacenter read as not tested rather
    than failed, and refuses a profile it cannot parse. -/
theorem jepsen_has_four_red_items : redCount ledger = 4 := by decide

def main : IO Unit :=
  printLedger "Jepsen/Elle: what a green run licenses" ledger

end Ferrosa.Testing.Jepsen
