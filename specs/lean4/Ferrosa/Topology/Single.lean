/-
  Ferrosa formal specification — standalone: one node, one copy, no quorum.
  =======================================================================

  One node is the whole cluster, so every write it acknowledges sits on the only copy
  that exists and losing that node loses the write with it.  At `RF = 1` there is no
  quorum arithmetic to get wrong: `QUORUM`, `ALL`, `LOCAL_QUORUM` and `ONE` all resolve
  to the same question, whether the node is up, and availability and durability are the
  same fact.  `EACH_QUORUM` is the one level the single-node path does not cover, because
  it is defined against a datacenter rather than against an `rf`.
  `Topology/Ladder.lean` takes this rung as the base case every escalation has to keep
  true: an acknowledged write stays readable, and nothing is acknowledged while the node
  is down.
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
