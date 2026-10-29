/-
  Ferrosa formal specification — Trace 2, rung 3: three nodes at RF = 2.
  ====================================================================

  `specs/reference/cluster-formation-state-machine.md` T6a says:

  > Quorum still holds (e.g., 1 of 3 nodes down — 2 remain, quorum = 2).
  > 3. Writes continue at CL=QUORUM (2 of 3 replicas available)

  That sentence silently assumes RF = 3.  At RF = 2 — the rung this ladder climbs
  first — one node loss removes the *only* quorum for half the keyspace.
  `rf2_has_no_fault_tolerance` is the machine-checked correction.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.ThreeNode

open Ferrosa
open Ferrosa.Topology

/-- 3 nodes, RF = 2, one DC.  Keys rotate over the three replica pairs. -/
def three : Topo where
  n := 3
  rf := 2
  dcOf := fun _ => 0
  replicasOf := fun k => [[0, 1], [1, 2], [0, 2]].getD (k % 3) [0, 1]

/-- `QUORUM` at RF = 2 means **both** replicas. -/
theorem rf2_quorum_is_both_replicas : blockFor .quorum 2 = 2 := by decide

/-- With one replica of the key down, `QUORUM` writes are refused. -/
theorem one_replica_down_blocks_quorum_write :
    three.okAtB (downOf [1]) 0 .quorum = false := by decide

/-- **The rung has no fault tolerance.**  *Every* single-node failure blocks
    `QUORUM` for at least one key. -/
theorem rf2_has_no_fault_tolerance :
    three.blocksSomeKey (failureSingles 3) (List.range 3) .quorum = true := by decide

/-- `QUORUM`/`QUORUM` is read-your-write at RF = 2: both operations must reach
    the same two replicas, and `W + R = 4 > 2`. -/
theorem quorum_quorum_is_read_your_write :
    pairsAll (subsets [0, 1])
      (fun a b => !(Nat.ble 2 a.length) || !(Nat.ble 2 b.length) || !(mayMiss a b)) = true := by
  decide

/-- `ONE`/`ONE` is **not** read-your-write at RF = 2: a write acked by one
    replica can be missed by a read served by the other. -/
theorem one_one_is_not_read_your_write : mayMiss [0] [1] = true := by decide

/-- `ALL` degrades to `QUORUM` at RF = 2 — no durability gain, only stricter
    availability loss. -/
theorem all_equals_quorum_at_rf2 : blockFor .all 2 = blockFor .quorum 2 := by decide

end Ferrosa.Topology.ThreeNode
