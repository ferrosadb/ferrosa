/-
  Ferrosa formal specification — Trace 2, rung 1: Standalone.
  ===========================================================

  `specs/reference/cluster-formation-state-machine.md`:
  "First node starts with no seed → Standalone", writes local only, DDL direct.

  With one copy there is no quorum arithmetic to get wrong: every consistency
  level collapses to "is the node up".  These theorems are the base case of the
  ladder and the reference for what escalation must preserve.
-/
import Ferrosa.Topology.Basic

namespace Ferrosa.Topology.Single

open Ferrosa
open Ferrosa.Topology

/-- One node, one replica, no DC split. -/
def single : Topo where
  n := 1
  rf := 1
  dcOf := fun _ => 0
  replicasOf := fun _ => [0]

def downNone : Down := fun _ => false
def downAll : Down := fun _ => true

/-- **Every CL is satisfied by the single node** — `QUORUM`, `ALL`, `LOCAL_QUORUM`
    all resolve to "the node is up". -/
theorem standalone_ok_at_every_level (cl : ConsistencyLevel) (hne : cl ≠ .eachQuorum) :
    single.okAt downNone 0 cl := by
  cases cl <;> simp_all [Topo.okAt, Topo.ackCount, Topo.liveReplicas, single, downNone,
                         blockFor, blockForDc]

/-- **With the node down nothing is acked** — there is no second copy to fall
    back to, so availability and durability are the same thing. -/
theorem standalone_down_acks_nothing (cl : ConsistencyLevel) (hne : cl ≠ .eachQuorum) :
    ¬ single.okAt downAll 0 cl := by
  cases cl <;> simp_all [Topo.okAt, Topo.ackCount, Topo.liveReplicas, single, downAll,
                         blockFor, blockForDc]

/-- A read can never miss an acked write: the ack set and the probe are the same
    single node. -/
theorem standalone_read_your_write : mayMiss [0] [0] = false := by decide

end Ferrosa.Topology.Single
