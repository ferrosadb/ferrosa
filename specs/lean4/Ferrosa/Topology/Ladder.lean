/-
  Ferrosa formal specification — Trace 2: the escalation ladder.
  =============================================================

  Single → Pair → 3-node RF=2 → 5-node RF=quorum → 2 DCs (one link) → 3 DC ring.

  Each rung is verified in its own file.  This file records the *ladder* itself:
  the threshold table, the fault-tolerance each rung actually buys, and the two
  properties that must survive every escalation — read-your-write and the
  impossibility of two disjoint write quorums.
-/
import Ferrosa.Topology.Basic
import Ferrosa.Topology.Pair
import Ferrosa.Topology.Single
import Ferrosa.Topology.ThreeNode
import Ferrosa.Topology.FiveNode
import Ferrosa.Topology.TwoDc
import Ferrosa.Topology.ThreeDcRing

namespace Ferrosa.Topology.Ladder

open Ferrosa
open Ferrosa.Topology

/-- The escalation rungs, in order. -/
inductive Rung where
  | standalone | pair | threeNode | fiveNode | twoDc | threeDcRing
  deriving DecidableEq, Repr

def Rung.nodes : Rung → Nat
  | .standalone => 1
  | .pair => 2
  | .threeNode => 3
  | .fiveNode => 5
  | .twoDc => 6
  | .threeDcRing => 9

def Rung.rf : Rung → Nat
  | .standalone => 1
  | .pair => 1
  | .threeNode => 2
  | .fiveNode => 3
  | .twoDc => 4
  | .threeDcRing => 6

/-- Replicas that may be lost while still forming a write quorum. -/
def Rung.faultTolerance (r : Rung) : Nat := r.rf - blockFor .quorum r.rf

def allRungs : List Rung :=
  [.standalone, .pair, .threeNode, .fiveNode, .twoDc, .threeDcRing]

/-- **The threshold table.**  `QUORUM` in acks, per rung. -/
theorem quorum_threshold_table :
    allRungs.map (fun r => blockFor .quorum r.rf) = [1, 1, 2, 2, 3, 4] := by decide

/-- **Fault tolerance per rung.**  Note what the requested third and fourth rungs
    actually buy: 3 nodes at RF=2 tolerate *nothing*, and 5 nodes at RF=3
    tolerate one replica — the same as the 2-DC rung. -/
theorem fault_tolerance_table :
    allRungs.map Rung.faultTolerance = [0, 0, 0, 1, 1, 2] := by decide

/-- Thresholds never decrease as the ladder climbs. -/
theorem escalation_thresholds_monotone :
    ((allRungs.map (fun r => blockFor .quorum r.rf)).zip
      ((allRungs.map (fun r => blockFor .quorum r.rf)).tail)).all
      (fun p => Nat.ble p.1 p.2) = true := by
  decide

/-- **Read-your-write survives every rung**: at each one, `QUORUM`+`QUORUM`
    satisfies `W + R > RF`. -/
theorem read_your_write_holds_at_every_rung :
    allRungs.all (fun r => 2 * blockFor .quorum r.rf > r.rf) = true := by decide

/-- **Pair mode has no quorum at all.**  A "quorum" of one cannot distinguish
    "the peer is dead" from "the network is partitioned", which is exactly why
    `Pair.no_two_writable` holds only because promotion is operator-gated. -/
theorem pair_has_no_quorum : blockFor .quorum 1 = 1 ∧ blockFor .quorum 2 = 2 := by decide

/-- **Three nodes is the first rung with an autonomous quorum.**  At RF = 3 a
    quorum of 2 can outvote a single failed or partitioned node, so Raft can
    elect without an operator. -/
theorem three_nodes_is_the_first_autonomous_rung :
    blockFor .quorum 3 < 3 ∧ blockFor .quorum 2 = 2 := by decide

/-- Escalation never widens a quorum's *share* of the replica set: the ack
    threshold grows more slowly than the RF. -/
theorem quorum_share_never_grows {rf rf' : Nat} (h : rf ≤ rf') :
    blockFor .quorum rf ≤ blockFor .quorum rf' := by
  simp only [blockFor]
  have : rf / 2 ≤ rf' / 2 := Nat.div_le_div_right h
  omega

/-- **No rung permits two disjoint write quorums.**  This is the invariant the
    whole ladder has to preserve, and it follows from the single counting lemma
    in `Ferrosa.Core` rather than from any per-rung case analysis. -/
theorem no_two_disjoint_quorums_at_any_rung (rf : Nat) (A B : SetU)
    (hA : IsQuorum rf A) (hB : IsQuorum rf B) :
    ∃ i, i < rf ∧ A i = true ∧ B i = true :=
  quorum_intersects hA hB

/-- The two-DC rung is the first that can serve `LOCAL_QUORUM` writes **on both
    sides of a cut**, which is also the first rung that can accept two divergent
    writes for the same key (`TwoDc.local_quorum_acks_can_be_disjoint`). -/
theorem local_quorum_needs_at_least_two_dcs :
    blockForDc .localQuorum 2 = 2 ∧ blockForDc .localQuorum 3 = 2 := by decide

end Ferrosa.Topology.Ladder
