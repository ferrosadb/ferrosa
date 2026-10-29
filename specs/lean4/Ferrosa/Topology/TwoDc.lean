/-
  Ferrosa formal specification — Trace 2, rung 5: two DCs, one WAN link.
  =====================================================================

  Topology: DC0 = {0,1,2}, DC1 = {3,4,5}, NetworkTopologyStrategy `{dc0: 2,
  dc1: 2}` (RF = 4), a single link carrying all cross-DC traffic.

  Sources: `specs/decisions/015-multi-dc-raft-per-dc-accord.md` (S-48: "DC
  partition steady state — each DC retains LOCAL availability; cross-DC blocked")
  and `ferrosa-cluster/src/consistency.rs`.

  Two results matter here:

  1. `local_quorum_survives_wan_cut` / `global_quorum_blocked_by_wan_cut` — the
     availability split ADR-015 promises, checked.
  2. `local_quorum_acks_can_be_disjoint` — LOCAL_QUORUM is an *availability*
     level, not a consistency level: with the WAN down, both DCs can ack a write
     to the same key with no replica in common.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.TwoDc

open Ferrosa
open Ferrosa.Topology

/-- 6 nodes, 3 per DC, RF = 4 (NTS 2 + 2). -/
def twoDc : Topo where
  n := 6
  rf := 4
  dcOf := dcBlocks 3 2
  replicasOf := fun k => [[0, 1, 3, 4], [1, 2, 4, 5], [2, 0, 5, 3]].getD (k % 3) [0, 1, 3, 4]

/-- The WAN link is down: every DC1 node is unreachable from DC0. -/
def wanCut : Down := downOf [3, 4, 5]

/-- Per-DC RF is 2, so LOCAL_QUORUM is 2 acks — both local replicas. -/
theorem nts_local_threshold_is_two : blockForDc .localQuorum 2 = 2 := by decide

/-- **DC0 keeps LOCAL_QUORUM availability through the WAN cut.** -/
theorem local_quorum_survives_wan_cut :
    (List.range 3).all (fun k => twoDc.localOkAtB wanCut 0 k .localQuorum 2) = true := by
  decide

/-- **DC1 keeps it too**, from its own side of the cut. -/
theorem local_quorum_survives_wan_cut_dc1 :
    (List.range 3).all (fun k => twoDc.localOkAtB (downOf [0, 1, 2]) 1 k .localQuorum 2) = true := by
  decide

/-- `QUORUM` over RF = 4 needs 3 acks; the cut leaves 2.  Cross-DC writes are
    refused rather than silently degraded. -/
theorem global_quorum_blocked_by_wan_cut :
    twoDc.blocksSomeKey [[3, 4, 5]] (List.range 3) .quorum = true := by decide

/-- `EACH_QUORUM` requires a local quorum in every DC, so one DC down blocks it
    even though the surviving DC is healthy. -/
theorem each_quorum_blocked_by_wan_cut :
    twoDc.localOkAtB wanCut 1 0 .eachQuorum 2 = false := by decide

/-- **LOCAL_QUORUM acks can be disjoint.**  During the cut, DC0 acks on `{0,1}`
    and DC1 on `{3,4}` for the same key: both writes are reported successful and
    no replica holds both.  Reconciliation is left to timestamps, and
    `Ferrosa.WritePath.lww_ties_are_order_dependent` shows equal timestamps do
    not converge. -/
theorem local_quorum_acks_can_be_disjoint : mayMiss [0, 1] [3, 4] = true := by decide

/-- **Feeding the global RF to a LOCAL level is a category error.**  On an NTS
    keyspace `{2,2}` the global RF is 4, so `block_for(LOCAL_QUORUM, 4)` asks for
    3 acks — but LOCAL_QUORUM is defined against the *local* RF, which is 2 and
    yields 2.  The global RF is not an input to a local level at all; see
    `Ferrosa.Topology.ConsistencyLevels` for the statement of that and for the
    corrected reading of the call sites (`write_path.rs:938` routes NTS to
    `coordinate_write_nts`, which computes the local threshold correctly). -/
theorem global_rf_local_quorum_would_over_ask :
    blockFor .localQuorum twoDc.rf > blockForDc .localQuorum 2 := by decide

/-- The two numbers in one statement: what the global-RF path would ask for,
    versus what the local DC can supply. -/
theorem local_quorum_threshold_depends_on_rf_used :
    blockFor .localQuorum 4 ≠ blockForDc .localQuorum 2 := by decide

/-- `LOCAL_ONE` computed on the global path is not local at all: it will ack on
    a remote replica, which is what `cl_routing` then filters away for the
    voter-quorum levels. -/
theorem local_one_global_path_is_not_local : blockFor .localOne 4 = 1 := by decide

end Ferrosa.Topology.TwoDc
