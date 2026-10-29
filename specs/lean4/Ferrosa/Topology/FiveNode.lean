/-
  Ferrosa formal specification — Trace 2, rung 4: five nodes at RF = quorum.
  ========================================================================

  "5 node RF = quorum" is ambiguous, and the ambiguity is load-bearing: a quorum
  of *replicas* at RF = 3 is 2, while a quorum of the *cluster* is 3.  Ferrosa
  computes `block_for(QUORUM, rf)` from the RF, so quorum means "2 of the 3
  replicas", not "3 of the 5 nodes".

  `quorum_is_over_replicas_not_cluster` and
  `cluster_majority_read_can_miss_replica_write` make that difference explicit.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.FiveNode

open Ferrosa
open Ferrosa.Topology

/-- 5 nodes, RF = 3 (the quorum of 5), one DC. -/
def five : Topo where
  n := 5
  rf := 3
  dcOf := fun _ => 0
  replicasOf := fun k => [[0, 1, 2], [1, 2, 3], [2, 3, 4], [3, 4, 0], [4, 0, 1]].getD (k % 5) [0, 1, 2]

/-- `QUORUM` at RF = 3 is two acks — exactly one replica of slack. -/
theorem rf3_quorum_is_two : blockFor .quorum 3 = 2 := by decide

/-- **One node may fail without losing any key.** -/
theorem rf3_survives_any_single_failure :
    five.survivesAll (singleFailures 5) (List.range 5) .quorum = true := by decide

/-- **Two simultaneous replica failures do block some key**, at RF = 3. -/
theorem rf3_blocked_by_two_replica_failures :
    five.blocksSomeKey (failurePairs 5) (List.range 5) .quorum = true := by decide

/-- `QUORUM`/`QUORUM` is read-your-write at RF = 3 (`2 + 2 > 3`). -/
theorem rf3_quorum_quorum_is_read_your_write :
    pairsAll (subsets [0, 1, 2])
      (fun a b => !(Nat.ble 2 a.length) || !(Nat.ble 2 b.length) || !(mayMiss a b)) = true := by
  decide

/-- **A cluster-majority read (3 of 5) can miss a replica-quorum write
    (2 of 3).**  The write acked on replicas `{0,1}`; a read that happens to
    consult only `{2,3,4}` sees node 2 — a replica, but not one that acked. -/
theorem cluster_majority_read_can_miss_replica_write :
    mayMiss [0, 1] [2, 3, 4] = true := by decide

/-- The two quorum bases genuinely differ at this rung: 2 of 3 replicas is not a
    majority of the 5-node cluster (`2 * 2 < 5`). -/
theorem quorum_is_over_replicas_not_cluster :
    blockFor .quorum 3 = 2 ∧ 2 * 2 < 5 := by decide

/-- Which is why a `SERIAL`/leader read is the only way to see the newest write
    at this rung without reading the quorum's own replica set. -/
theorem quorum_and_cluster_majority_differ : blockFor .quorum 3 ≠ blockFor .quorum 5 := by decide

end Ferrosa.Topology.FiveNode
