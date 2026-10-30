/-
  Ferrosa formal specification — three datacenters in a ring: one may go, two may not.
  ==================================================================================

  Three datacenters of three nodes each at `{2,2,2}` is the shape that survives the loss
  of any one whole datacenter: a quorum of six needs four acks, and the two surviving DCs
  supply exactly four — which is the property `specs/decisions/015-multi-dc-raft-per-dc-accord.md`
  is buying with a per-DC replication factor.  It does not survive two: one DC
  contributes two replicas against a threshold of four, so cross-DC writes stop instead of
  under-acking, and no single DC holds enough replicas to carry a quorum on its own.  The
  ring wiring buys the same kind of margin one level down — any one WAN link may fail and
  all three DCs stay mutually reachable, two failures isolate at most one DC, and the two
  that remain still talk to each other.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.ThreeDcRing

open Ferrosa
open Ferrosa.Topology

/-! ## Which datacenters can still reach each other once a link is cut -/

/-- A direct WAN link between DCs `a` and `b`, in either direction. -/
def dcLink (links : List (Nat × Nat)) (a b : Nat) : Bool :=
  links.contains (a, b) || links.contains (b, a)

/-- Reachability in at most two hops — enough for a 3-node DC graph. -/
def dcReach (links : List (Nat × Nat)) (a b : Nat) : Bool :=
  (a == b) || dcLink links a b ||
    ((List.range 3).any (fun c => dcLink links a c && dcLink links c b))

/-- All three DCs mutually reachable. -/
def dcConnected (links : List (Nat × Nat)) : Bool :=
  (List.range 3).all (fun a => (List.range 3).all (fun b => dcReach links a b))

/-- The ring: 0–1, 1–2, 2–0. -/
def ringLinks : List (Nat × Nat) := [(0, 1), (1, 2), (0, 2)]

/-- The links still up after removing `cut`. -/
def afterCut (cut : List (Nat × Nat)) : List (Nat × Nat) :=
  ringLinks.filter (fun e => !(cut.contains e))

/-- **Any single link loss leaves the ring connected.** -/
theorem ring_survives_one_link_loss :
    (subsets ringLinks).all (fun cut => Nat.ble 2 cut.length || dcConnected (afterCut cut)) = true := by
  decide

/-- **After any one link loss the ring is still fully connected** — that is what
    a ring buys over a star. -/
theorem ring_survives_one_loss_fully :
    (subsets ringLinks).all (fun cut => Nat.ble 2 cut.length || dcConnected (afterCut cut)) = true := by
  decide

/-- Losing two of the three links isolates one DC, but **some cross-DC path
    always survives** — the two remaining DCs can still talk. -/
theorem two_link_loss_isolates_at_most_one_dc :
    (subsets ringLinks).all (fun cut =>
      Nat.ble 3 cut.length ||
        (List.range 3).any (fun a => (List.range 3).any (fun b =>
          (a != b) && dcReach (afterCut cut) a b))) = true := by
  decide

/-- Losing all three links disconnects the DCs. -/
theorem ring_disconnects_when_fully_cut : dcConnected (afterCut ringLinks) = false := by decide

/-- Isolating DC2 leaves DC0 and DC1 connected to each other. -/
theorem isolating_one_dc_keeps_the_other_two_connected :
    dcReach (afterCut [(1, 2), (0, 2)]) 0 1 = true ∧
      dcReach (afterCut [(1, 2), (0, 2)]) 1 2 = false := by
  decide

/-! ## What a quorum of six needs, and which datacenter losses it survives -/

/-- 9 nodes, 3 per DC, RF = 6 (NTS 2 + 2 + 2). -/
def threeDc : Topo where
  n := 9
  rf := 6
  dcOf := dcBlocks 3 3
  replicasOf := fun k =>
    [[0, 1, 3, 4, 6, 7], [1, 2, 4, 5, 7, 8], [2, 0, 5, 3, 8, 6]].getD (k % 3)
      [0, 1, 3, 4, 6, 7]

/-- `QUORUM` at RF = 6 is four acks. -/
theorem rf6_quorum_is_four : blockFor .quorum 6 = 4 := by decide

/-- **No single DC can carry a cross-DC quorum**: a DC holds 3 replicas but 4
    are needed, so every quorum spans at least two DCs. -/
theorem quorum_of_six_spans_two_dcs : blockFor .quorum 6 > 3 := by decide

/-- **A single DC loss keeps `QUORUM` for every key.**  With 2 replicas per DC
    and RF = 6, any two surviving DCs contribute exactly the 4 acks a quorum of
    6 needs.  This is the property ADR-015 is buying with NTS `{2,2,2}`. -/
theorem single_dc_loss_keeps_quorum_for_every_key :
    threeDc.survivesAll [[0, 1, 2], [3, 4, 5], [6, 7, 8]] (List.range 3) .quorum = true := by
  decide

/-- **Two DC losses do not.**  One surviving DC contributes 2 replicas against a
    threshold of 4, so cross-DC writes stop rather than silently under-acking. -/
theorem two_dc_loss_breaks_quorum :
    threeDc.blocksSomeKey [[0, 1, 2, 6, 7, 8]] (List.range 3) .quorum = true := by
  decide

/-- `EACH_QUORUM` never survives an isolated DC: it demands a local quorum in
    the DC that is gone. -/
theorem each_quorum_never_survives_dc_loss :
    threeDc.localOkAtB (downOf [6, 7, 8]) 2 0 .eachQuorum 2 = false := by decide

/-! ## Where the redundancy stops: one datacenter may go, two may not -/

/-- The ring tolerates exactly one DC loss: one is free, two take `QUORUM` with
    them.  That is the boundary of the last rung. -/
theorem ring_tolerates_exactly_one_dc_loss :
    threeDc.survivesAll [[0, 1, 2], [3, 4, 5], [6, 7, 8]] (List.range 3) .quorum = true ∧
      threeDc.blocksSomeKey [[0, 1, 2, 6, 7, 8]] (List.range 3) .quorum = true := by
  decide

end Ferrosa.Topology.ThreeDcRing