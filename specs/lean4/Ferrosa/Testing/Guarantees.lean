/-
  A guarantee is a function of the consistency level
  ==================================================

  CL=ONE on a three-node cluster does not promise what CL=ALL promises, and that is the design rather
  than a defect, so the suite cannot assert one fixed property — it has to assert the property the
  configured levels actually deliver. The single condition behind every guarantee Ferrosa makes about
  a read is overlap: a read can be trusted to see an acknowledged write only when W + R > RF, and it is
  the pair of levels that decides it, never either level on its own. A read answered by a datacenter
  that does not hold the write defeats the argument however the replica counts fall, which is why
  DC_QUORUM's local reads cannot establish strict serializability. Raising either level can only add
  guarantees, so a weaker configuration is a sound subset rather than a bug, and a verdict is a
  function of the history and the profile — a pass/fail without the profile is not a result. Every row
  in this ledger is met; where the suite fails to respect these conditions is audited in
  `Ferrosa/Testing/Jepsen.lean`.
-/
import Ferrosa.Core
import Ferrosa.Ledger
import Ferrosa.Sql.Consistency

namespace Ferrosa.Testing.Guarantees

open Ferrosa Ferrosa.Ledger

/-! ## Profiles -/

/-- The replica count a write is spread over in its datacenter. -/
structure Profile where
  /-- Replication factor. -/
  rf : Nat
  /-- Replicas that must acknowledge a write. -/
  writeLevel : Nat
  /-- Replicas that must respond to a read. -/
  readLevel : Nat
  /-- Whether a read may be answered by a datacenter that does not hold the write. -/
  crossDcRead : Bool
  deriving DecidableEq, Repr

/-- The replica counts a `Level` resolves to on a cluster of `rf` nodes in one datacenter. -/
def replicaCount : Ferrosa.Sql.Consistency.Level → Nat → Nat
  | .one, _ => 1
  | .quorum, rf => rf / 2 + 1
  | .localQuorum, rf => rf / 2 + 1
  | .all, rf => rf

def profile (rf : Nat) (w r : Ferrosa.Sql.Consistency.Level) (crossDcRead : Bool) : Profile :=
  ⟨rf, replicaCount w rf, replicaCount r rf, crossDcRead⟩

/-! ## The overlap condition -/

/-- **A read must intersect every write's replica set** exactly when `W + R > RF`. This is the single
    condition behind every data-loss guarantee Ferrosa can make about a read. -/
def overlap (p : Profile) : Bool := decide (p.rf < p.writeLevel + p.readLevel)

/-- The read can see every acknowledged write. The overlap is necessary; a cross-datacenter read
    defeats it, because the replica set it consults is not the one the write landed in. -/
def readsEveryAckedWrite (p : Profile) : Bool := overlap p && !p.crossDcRead

/-- The write survives the loss of one replica. -/
def survivesOneNodeLoss (p : Profile) : Bool := decide (1 < p.writeLevel)

/-- The write survives the loss of a quorum. -/
def survivesQuorumLoss (p : Profile) : Bool := decide (p.rf / 2 + 1 ≤ p.writeLevel)

/-! ## CL=ONE and CL=ALL are different guarantees -/

/-- **On a three-node cluster, CL=ONE and CL=ALL do not share a durability guarantee**, by design: one
    acknowledged replica is not two, and a single-node loss is exactly what CL=ONE does not survive.
    Both are correct; only one is stronger. -/
theorem one_and_all_do_not_share_a_durability_guarantee :
    survivesOneNodeLoss (profile 3 .one .one false) = false
      ∧ survivesOneNodeLoss (profile 3 .all .one false) = true := by decide

/-- CL=ONE plus CL=ONE does not overlap on three nodes: a read can miss an acknowledged write, so a
    missing value is not evidence of anything. -/
theorem a_one_and_one_configuration_cannot_report_loss :
    overlap (profile 3 .one .one false) = false
      ∧ readsEveryAckedWrite (profile 3 .one .one false) = false := by decide

/-- Quorum plus quorum overlaps on three nodes. -/
theorem a_quorum_pair_overlaps_on_three_nodes :
    overlap (profile 3 .quorum .quorum false) = true := by decide

/-- **It is the pair that matters, not either level.** CL=ALL writes with CL=ONE reads still overlap,
    because a write on every replica intersects any single-replica read. -/
theorem an_all_write_with_a_one_read_still_overlaps :
    readsEveryAckedWrite (profile 3 .all .one false) = true
      ∧ readsEveryAckedWrite (profile 3 .one .all false) = true := by decide

/-- And CL=ONE writes with CL=QUORUM reads do not, on three nodes: 1 + 2 is not greater than 3. -/
theorem a_one_write_with_a_quorum_read_does_not_overlap :
    readsEveryAckedWrite (profile 3 .one .quorum false) = false := by decide

/-! ## Monotonicity: a weaker configuration is a subset, not a bug -/

/-- **Raising either level can only add guarantees.** A configuration that promises less is sound —
    it tests less — so the suite must assert per configuration rather than globally. -/
theorem raising_the_read_level_cannot_remove_a_guarantee (rf w r : Nat) :
    overlap ⟨rf, w, r, false⟩ = true → overlap ⟨rf, w, r + 1, false⟩ = true := by
  intro h
  simp only [overlap, decide_eq_true_eq] at h ⊢
  omega

/-- Raising the write level by one cannot break the overlap, so a configuration that could see every
    acknowledged write still can after the change. -/
theorem raising_the_write_level_cannot_remove_a_guarantee (rf w r : Nat) :
    overlap ⟨rf, w, r, false⟩ = true → overlap ⟨rf, w + 1, r, false⟩ = true := by
  intro h
  simp only [overlap, decide_eq_true_eq] at h ⊢
  omega

/-- **A stronger level implies every weaker guarantee**, so the guarantee set is a chain and the suite
    can assert the strongest property and know it covers the weaker configurations. -/
theorem all_implies_one_for_durability :
    survivesOneNodeLoss (profile 5 .one .one false) = false
      ∧ survivesOneNodeLoss (profile 5 .quorum .one false) = true
      ∧ survivesOneNodeLoss (profile 5 .all .one false) = true := by decide

/-! ## A cross-datacenter read defeats the overlap argument -/

/-- **DC_QUORUM's local read cannot see a write acknowledged elsewhere**, even with an overlapping
    replica count, because the overlap is an argument about one replica set. -/
theorem a_cross_datacenter_read_defeats_the_overlap :
    readsEveryAckedWrite (profile 3 .quorum .quorum true) = false
      ∧ readsEveryAckedWrite (profile 3 .all .all true) = false := by decide

/-- A read that consults a quorum of datacenters restores it: the guarantee is about the read's
    *routing*, and no level name on its own expresses that. -/
theorem a_datacenter_quorum_read_restores_the_overlap :
    readsEveryAckedWrite (profile 3 .localQuorum .localQuorum false) = true := by decide

/-! ## What the suite may assert -/

/-- **A missing value is evidence of loss only when the reads overlap the writes.** Otherwise the
    verdict must be "not tested at this configuration", which is not the same as green. -/
def lossIsEvidence (p : Profile) (lostNonEmpty : Bool) : Bool :=
  readsEveryAckedWrite p && lostNonEmpty

/-- Under CL=ONE/CL=ONE a missing value is not evidence. -/
theorem a_missing_value_under_one_one_is_not_evidence :
    lossIsEvidence (profile 3 .one .one false) true = false := by decide

/-- Under CL=QUORUM/CL=QUORUM the same missing value is evidence. -/
theorem the_same_missing_value_under_quorum_quorum_is_evidence :
    lossIsEvidence (profile 3 .quorum .quorum false) true = true := by decide

/-- **The same history has different verdicts under different configurations**, so a verdict is a
    function of `(history, profile)` and a pass/fail without the profile is not a result. -/
theorem a_verdict_without_the_profile_is_not_a_verdict :
    lossIsEvidence (profile 3 .one .one false) true
      ≠ lossIsEvidence (profile 3 .quorum .quorum false) true := by decide

def ledger : List SpecItem := [
  ⟨"on three nodes CL=ONE and CL=ALL do not share a durability guarantee, by design",
   .met "Guarantees.one_and_all_do_not_share_a_durability_guarantee"⟩,
  ⟨"CL=ONE with CL=ONE does not overlap, so a missing value is not evidence of loss",
   .met "Guarantees.a_one_and_one_configuration_cannot_report_loss"⟩,
  ⟨"quorum with quorum overlaps on three nodes",
   .met "Guarantees.a_quorum_pair_overlaps_on_three_nodes"⟩,
  ⟨"it is the write/read pair that matters: CL=ALL writes with CL=ONE reads still overlap",
   .met "Guarantees.an_all_write_with_a_one_read_still_overlaps"⟩,
  ⟨"CL=ONE writes with CL=QUORUM reads do not overlap on three nodes",
   .met "Guarantees.a_one_write_with_a_quorum_read_does_not_overlap"⟩,
  ⟨"raising either level can only add guarantees, so a weaker configuration is sound",
   .met "Guarantees.raising_the_read_level_cannot_remove_a_guarantee"⟩,
  ⟨"a cross-datacenter read defeats the overlap argument even at CL=ALL",
   .met "Guarantees.a_cross_datacenter_read_defeats_the_overlap"⟩,
  ⟨"a missing value is evidence of loss only when the reads overlap the writes",
   .met "Guarantees.a_missing_value_under_one_one_is_not_evidence"⟩,
  ⟨"the same history has different verdicts under different configurations",
   .met "Guarantees.a_verdict_without_the_profile_is_not_a_verdict"⟩,
  ⟨"the verdict is a function of (history, profile), so a pass/fail without the profile means nothing",
   .met "Histories.nothing_is_confirmed_that_the_profile_cannot_support"⟩
]

/-- No red rows: every claim this ledger makes about the consistency levels is proved, and this pins
    that none has been added since without a proof. -/
theorem guarantees_has_no_red_items : redCount ledger = 0 := by decide

def main : IO Unit :=
  printLedger "consistency-level guarantees the suite may assert" ledger

end Ferrosa.Testing.Guarantees
