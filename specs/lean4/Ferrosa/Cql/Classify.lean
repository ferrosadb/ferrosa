/-
  Ferrosa formal specification — CQL front end, part 2: how many replicas must
  acknowledge, and what a transaction touches.
  ==============================================================================

  A statement and the keyspace's replication strategy are enough to decide how
  many replicas must acknowledge it: one level names the coordinating
  datacenter, another asks for a quorum in every datacenter, and the plan says
  which. Those two shapes are not interchangeable, and the requirement is per
  datacenter rather than a total — five acknowledgements all in the primary do
  not stand in for three there and two in the secondary. What a single
  acknowledgement means is `Ferrosa/Topology/ConsistencyLevels.lean`; counting a
  whole statement's worth of them is here.

  A run of statements between `BEGIN` and `COMMIT` is summarised the same way: the
  keys it reads, the keys it writes, and how many conditions it tests. A
  transaction containing any conditional statement is never blind, so it is never
  acknowledged on the write path alone, and the coordinating datacenter is called
  `localDc` throughout because `local` is a Lean keyword.
-/
import Ferrosa.Cql.Syntax

namespace Ferrosa.Cql

open Ferrosa

/-! ## How a keyspace spreads its replicas, and how many it has

A keyspace says two things: how many copies of each key exist, and where they live. `Simple` puts a
fixed number of copies on the ring and does not care which datacenter they land in; `NetworkTopology`
names a replication factor per datacenter, so a keyspace written for three copies in the east and
two in the west holds five in total and never fewer than three in the east — the second number is
what a `LOCAL_QUORUM` read depends on, and it cannot be recovered from the total.

Both are modelled as data rather than as a placement function: the two counts below are all the rest
of this module needs from a strategy, and keeping the strategy opaque is what lets an ack plan be
computed without knowing how a partitioner lays keys on the ring. -/

abbrev DcId := String

inductive Strategy where
  | simple (rf : Nat)
  | nts (dcs : List (DcId × Nat))
  deriving DecidableEq, Repr

/-- Total replicas per key: the RF for `Simple`, the sum of the per-DC factors for
    NTS (`ferrosa-cluster/src/ring/strategy.rs:20`). -/
def Strategy.globalRf : Strategy → Nat
  | .simple rf => rf
  | .nts dcs => (dcs.map (·.2)).sum

/-- The replication factor of one DC.  For `Simple` every DC is "the" DC. -/
def Strategy.dcRf : Strategy → DcId → Nat
  | .simple rf, _ => rf
  | .nts dcs, d => (dcs.lookup d).getD 0

def Strategy.dcs : Strategy → List DcId
  | .simple _ => []
  | .nts dcs => dcs.map (·.1)

/-! ## How many acknowledgements a level requires, and in which datacenters

`total` and `perDc` are not interchangeable: `total ≥ n` with the wrong
distribution is a different requirement from a per-DC one, which is exactly the
gap recorded in `FINDINGS.md` A3. -/

structure AckPlan where
  total : Nat
  perDc : List (DcId × Nat)
  deriving DecidableEq, Repr

/-- The plan the specification intends: local levels are per DC, and
    `QUORUM`/`SERIAL` on an NTS keyspace is a quorum in *every* DC. -/
def planFor (cl : ConsistencyLevel) (s : Strategy) (localDc : DcId) : AckPlan :=
  match cl with
  | .localOne =>
      { total := 1, perDc := [(localDc, 1)] }
  | .localQuorum | .localSerial =>
      { total := s.dcRf localDc / 2 + 1, perDc := [(localDc, s.dcRf localDc / 2 + 1)] }
  | .eachQuorum =>
      { total := (s.dcs.map (fun d => s.dcRf d / 2 + 1)).sum,
        perDc := s.dcs.map (fun d => (d, s.dcRf d / 2 + 1)) }
  | .quorum | .serial =>
      match s with
      | .simple rf => { total := rf / 2 + 1, perDc := [] }
      | .nts dcs =>
          { total := (dcs.map (fun p => p.2 / 2 + 1)).sum,
            perDc := dcs.map (fun p => (p.1, p.2 / 2 + 1)) }
  | .one | .two | .three | .all =>
      { total := blockFor cl s.globalRf, perDc := [] }

/-! ### The plan's properties, and where the code's plan differs -/

/-- A keyspace holding one replica needs one acknowledgement at `ONE`, so the
    level adds nothing to what the topology already requires. -/
theorem blockFor_one_at_one : blockFor .one 1 = 1 := by decide

/-- With one replica, `ALL` asks for that replica and so for a single
    acknowledgement — the same number as `ONE`. -/
theorem blockFor_all_at_one : blockFor .all 1 = 1 := by decide

/-- `RF = 1` needs one ack at every level that applies to a single-DC keyspace —
    the front end adds nothing to the topology rule.  (`EACH_QUORUM` is excluded:
    it has no meaning on a `Simple` keyspace, which has no DCs.) -/
theorem plan_rf_one_single_ack (localDc : DcId) :
    (planFor .one (.simple 1) localDc).total = 1 ∧
      (planFor .quorum (.simple 1) localDc).total = 1 ∧
      (planFor .localQuorum (.simple 1) localDc).total = 1 ∧
      (planFor .localOne (.simple 1) localDc).total = 1 ∧
      (planFor .all (.simple 1) localDc).total = 1 := by
  refine ⟨?_, ?_, ?_, ?_, ?_⟩
  · simpa [planFor, Strategy.globalRf] using blockFor_one_at_one
  · simp [planFor, Strategy.globalRf]
  · simp [planFor, Strategy.dcRf]
  · rfl
  · simpa [planFor, Strategy.globalRf] using blockFor_all_at_one

/-- `LOCAL_QUORUM` names exactly one DC and requires that DC's quorum. -/
theorem plan_local_quorum_is_local (s : Strategy) (localDc : DcId) :
    (planFor .localQuorum s localDc).perDc = [(localDc, s.dcRf localDc / 2 + 1)] := by
  cases s <;> rfl

/-- `QUORUM` on NTS requires a quorum **in each DC**, and the total is the sum of
    those — not a global majority. -/
theorem plan_quorum_nts_is_per_dc (dcs : List (DcId × Nat)) (localDc : DcId) :
    (planFor .quorum (.nts dcs) localDc).perDc = dcs.map (fun p => (p.1, p.2 / 2 + 1))
      ∧ (planFor .quorum (.nts dcs) localDc).total
          = (dcs.map (fun p => p.2 / 2 + 1)).sum := by
  exact ⟨rfl, rfl⟩

/-- **Where the implementation's plan differs from the specified one.**  The code
    computes `QUORUM` as `block_for(QUORUM, globalRf)` — one global majority — and
    checks `total_acks >= required`.  That agrees with the per-DC plan exactly
    when the per-DC RFs are odd; on `{2,2}` it asks for 3 where the per-DC plan
    asks 4, and on `{3,3}` the count agrees but the distribution is unconstrained. -/
theorem code_plan_agrees_only_for_odd_dc_rf :
    blockFor .quorum 6 = (planFor .quorum (.nts [("dc1", 3), ("dc2", 3)]) "dc1").total ∧
      blockFor .quorum 4 < (planFor .quorum (.nts [("dc1", 2), ("dc2", 2)]) "dc1").total := by
  decide

/-- A level that names a DC always produces a per-DC requirement. -/
theorem local_levels_have_a_dc_requirement (s : Strategy) (localDc : DcId) :
    (planFor .localQuorum s localDc).perDc ≠ [] ∧ (planFor .localOne s localDc).perDc ≠ [] := by
  cases s <;> simp [planFor]

/-- `ALL` on NTS requires every replica, whatever the level's "quorum" arithmetic
    would have said. -/
theorem plan_all_is_total (dcs : List (DcId × Nat)) (localDc : DcId) :
    (planFor .all (.nts dcs) localDc).total = (dcs.map (·.2)).sum := by
  simp [planFor, Strategy.globalRf, blockFor]

/-! ## Which level a statement actually runs at

The frame may carry a level; if it does not, the session/keyspace default
applies.  Two properties matter: resolution never overrides an explicit choice,
and it never invents one. -/

def resolveCl (dflt : ConsistencyLevel) (q : Query) : ConsistencyLevel :=
  q.cl.getD dflt

/-- A level carried in the frame is used as sent: the session default never
    overrides a client's explicit choice. -/
theorem resolveCl_of_some (dflt cl : ConsistencyLevel) (stmt : Statement) :
    resolveCl dflt ⟨stmt, some cl⟩ = cl := rfl

/-- A query that names no level runs at the session default, so a client that
    says nothing gets the configured level rather than an error. -/
theorem resolveCl_of_none (dflt : ConsistencyLevel) (stmt : Statement) :
    resolveCl dflt ⟨stmt, none⟩ = dflt := rfl

/-! ## What a run of statements between `BEGIN` and `COMMIT` touches -/

structure TxnShape where
  reads : List Key
  writes : List Key
  conditions : Nat
  deriving DecidableEq, Repr

def TxnShape.nil : TxnShape := ⟨[], [], 0⟩

def TxnShape.union (a b : TxnShape) : TxnShape :=
  ⟨a.reads ++ b.reads, a.writes ++ b.writes, a.conditions + b.conditions⟩

/-- The read/write footprint of one statement.  Key lists are supplied by the
    caller: `transaction_keys.rs` computes them from the terms, and doing it here
    would drag token hashing into the spec for no gain. -/
def Stmt.shape (readKeys writeKeys : Stmt → List Key) (s : Stmt) : TxnShape :=
  ⟨(if s.conditional then readKeys s else []),
   (if s.writes then writeKeys s else []),
   (if s.conditional then 1 else 0)⟩

/-- A group of statements, as delimited by `BEGIN … COMMIT`. -/
def txnShape (readKeys writeKeys : Stmt → List Key) (ss : List Stmt) : TxnShape :=
  ss.foldl (fun acc s => acc.union (s.shape readKeys writeKeys)) TxnShape.nil

/-- A conditional statement reads the keys it compares against. -/
theorem conditional_reads_its_keys (readKeys writeKeys : Stmt → List Key) (s : Stmt)
    (h : s.conditional = true) : (s.shape readKeys writeKeys).reads = readKeys s := by
  simp [Stmt.shape, h]

/-- And it contributes exactly one condition. -/
theorem conditional_contributes_condition (readKeys writeKeys : Stmt → List Key) (s : Stmt)
    (h : s.conditional = true) : 0 < (s.shape readKeys writeKeys).conditions := by
  simp [Stmt.shape, h]

/-- An unconditional write contributes no condition. -/
theorem unconditional_has_no_condition (readKeys writeKeys : Stmt → List Key) (s : Stmt)
    (h : s.conditional = false) : (s.shape readKeys writeKeys).conditions = 0 := by
  simp [Stmt.shape, h]

/-- A group of statements tests a condition exactly when one of its members does,
    which is what lets a batch's shape be read off its statements. -/
theorem any_conditional_iff (ss : List Stmt) :
    ss.any Stmt.conditional = true ↔ ∃ s ∈ ss, Stmt.conditional s = true := by
  simp [List.any_eq_true]

/-- **LWT needs a read phase.**  A transaction containing a conditional statement
    is not blind: it cannot be acknowledged on the write path alone.  This is the
    front-end half of "LWT is a transaction"; `Lwt.lean` supplies the protocol
    half. -/
theorem conditional_txn_not_blind (ss : List Stmt)
    (h : ss.any Stmt.conditional = true) : ss.all Stmt.blind = false := by
  obtain ⟨s, hmem, hcond⟩ := (any_conditional_iff ss).mp h
  have hb : Stmt.blind s = false := by
    cases s with
    | insert t v ine => simp only [Stmt.blind, Stmt.conditional] at *; cases ine <;> simp_all
    | update t a w ie cs => simp only [Stmt.blind, Stmt.conditional] at *; cases ie <;> simp_all
    | delete t w ie cs => simp only [Stmt.blind, Stmt.conditional] at *; cases ie <;> simp_all
    | select t w => simp [Stmt.blind]
    | beginTxn => simp [Stmt.conditional] at hcond
    | commit => simp [Stmt.conditional] at hcond
    | rollback => simp [Stmt.conditional] at hcond
    | other => simp [Stmt.conditional] at hcond
  have hne : ss.all Stmt.blind ≠ true := by
    intro ht
    have h1 : Stmt.blind s = true := List.all_eq_true.mp ht s hmem
    rw [hb] at h1
    exact Bool.false_ne_true h1
  simpa using hne

end Ferrosa.Cql
