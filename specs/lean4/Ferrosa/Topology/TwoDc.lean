/-
  Ferrosa formal specification — two datacenters, one WAN link, and what a cut costs.
  =================================================================================

  Two datacenters at `{2,2}` behind a single WAN link trade cross-datacenter consistency
  for local availability: with the link down each DC keeps accepting `LOCAL_QUORUM`
  writes and serving its own reads, while `QUORUM` writes are refused rather than
  silently under-acked.  `LOCAL_QUORUM` is an availability level and not a consistency
  level — during a cut both DCs can acknowledge a write to the same key with no replica
  in common, and the two values are left for timestamp reconciliation.
  `EACH_QUORUM` is the level that refuses the trade: it needs a local quorum in every
  datacenter, so one DC down blocks it even though the survivor is healthy.  Feeding the
  *global* replication factor to a local level would ask three acks where the local DC
  can only supply two; `Topology/ConsistencyLevels.lean` states why the global factor is
  never an input to a local level.
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
