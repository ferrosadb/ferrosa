/-
  Placement as data: one shape for every rung of the failure ladder
  ================================================================

  Each rung a deployment can be on — a single node, a pair, three nodes at replication factor 2, five
  at quorum, two datacenters joined by one WAN link — is described by the same four pieces of data,
  so a claim about availability is a claim about a `Topo` and nothing else. Admission is computed from
  that data rather than asserted: a request is served when the live replicas reach `blockFor rf`, and
  refused when they do not.

  The Boolean mirrors and the failure-pattern enumerations exist so that each rung's claims can be
  checked by `decide` over every failure shape rather than argued one case at a time.
-/
import Ferrosa.Core

namespace Ferrosa.Topology

open Ferrosa

/-- A concrete placement: `n` nodes, `rf` replicas per key, a DC map, and a
    per-key replica list (already `rf` long). -/
structure Topo where
  n : Nat
  rf : Nat
  dcOf : Node → Nat
  replicasOf : Node → List Node

/-- Which nodes are down. -/
abbrev Down := SetU

/-- The replicas of `key` that are up. -/
def Topo.liveReplicas (t : Topo) (down : Down) (key : Node) : List Node :=
  (t.replicasOf key).filter (fun i => !(down i))

def Topo.ackCount (t : Topo) (down : Down) (key : Node) : Nat :=
  (t.liveReplicas down key).length

/-- A write (or read) at consistency level `cl` is admitted iff the number of
    live replicas reaches `block_for(rf)`.  This is `write.rs:295` /
    `read.rs:573` with the replicas already resolved by the ring. -/
def Topo.okAt (t : Topo) (down : Down) (key : Node) (cl : ConsistencyLevel) : Prop :=
  blockFor cl t.rf ≤ t.ackCount down key

/-- Replicas of `key` that live in `dc`. -/
def Topo.localReplicas (t : Topo) (dc : Nat) (key : Node) : List Node :=
  (t.replicasOf key).filter (fun i => t.dcOf i == dc)

/-- LOCAL_* admission: the threshold is computed from the **per-DC** RF, which
    is what `write.rs:612` and `read.rs:1205` do on the NTS paths. -/
def Topo.localOkAt (t : Topo) (down : Down) (dc : Nat) (key : Node)
    (cl : ConsistencyLevel) (dcRf : Nat) : Prop :=
  blockForDc cl dcRf ≤ ((t.localReplicas dc key).filter (fun i => !(down i))).length

/-- `true` when a client that reads only from `probe` can miss a write that was
    acked by `ack` — i.e. the two node sets are disjoint. -/
def mayMiss (ack probe : List Node) : Bool :=
  ack.all (fun i => !(probe.contains i))

/-- Two node sets share a member. -/
def overlaps (a b : List Node) : Bool :=
  a.any (fun i => b.contains i)

/-- A quorum at level `cl` over an `rf`-sized replica set. -/
def quorumOf (rf : Nat) (cl : ConsistencyLevel) (ack : List Node) : Prop :=
  blockFor cl rf ≤ ack.length

/-! ## Node / DC helpers for the concrete rungs -/

/-- `k` consecutive nodes starting at `start`. -/
def block (start k : Nat) : List Node := (List.range k).map (· + start)

/-- DC map: `perDc` nodes per DC, numbered DC 0, 1, 2 … -/
def dcBlocks (perDc : Nat) (_nDc : Nat) : Node → Nat :=
  fun i => i / perDc

/-- Powerset of a list, for enumerating failure patterns and link cuts. -/
def subsets {α : Type} : List α → List (List α)
  | [] => [[]]
  | x :: xs =>
      let r := subsets xs
      r ++ r.map (fun s => x :: s)

/-- Every failure pattern over `{0, …, n-1}`. -/
def downSets (n : Nat) : List (List Node) := subsets (List.range n)

/-- A failure pattern as a `Down` predicate. -/
def downOf (l : List Node) : Down := fun i => l.contains i

/-- At most one node failed. -/
def singleFailures (n : Nat) : List (List Node) := [] :: (List.range n).map (fun i => [i])

/-- Bool mirror of `okAt`, so failure patterns can be enumerated with `decide`. -/
def Topo.okAtB (t : Topo) (down : Down) (key : Node) (cl : ConsistencyLevel) : Bool :=
  Nat.ble (blockFor cl t.rf) (t.ackCount down key)

/-- The Boolean admission test agrees with the propositional one for every placement, failure
    pattern and consistency level, so an answer obtained by enumerating failures is an answer about
    the predicate and not a separate rule that could drift from it. -/
theorem Topo.okAtB_iff {t : Topo} {down : Down} {key : Node} {cl : ConsistencyLevel} :
    t.okAtB down key cl = true ↔ t.okAt down key cl := by
  simp [Topo.okAtB, Topo.okAt, Nat.ble_eq]

/-- Bool mirror of `localOkAt`. -/
def Topo.localOkAtB (t : Topo) (down : Down) (dc : Nat) (key : Node)
    (cl : ConsistencyLevel) (dcRf : Nat) : Bool :=
  Nat.ble (blockForDc cl dcRf)
    ((t.localReplicas dc key).filter (fun i => !(down i))).length

/-- The same agreement for the per-datacenter gate, so a failure pattern enumerated for a `LOCAL_*`
    level answers the question `localOkAt` asks. -/
theorem Topo.localOkAtB_iff {t : Topo} {down : Down} {dc : Nat} {key : Node}
    {cl : ConsistencyLevel} {dcRf : Nat} :
    t.localOkAtB down dc key cl dcRf = true ↔ t.localOkAt down dc key cl dcRf := by
  simp [Topo.localOkAtB, Topo.localOkAt, Nat.ble_eq]

/-- Failure patterns with exactly one failed node. -/
def failureSingles (n : Nat) : List (List Node) := (List.range n).map (fun i => [i])

/-- Failure patterns with exactly two failed nodes. -/
def failurePairs (n : Nat) : List (List Node) :=
  (downSets n).filter (fun l => l.length == 2)

/-- `true` iff every failure pattern in `pats` blocks `cl` for some key in `keys`. -/
def Topo.blocksSomeKey (t : Topo) (pats : List (List Node)) (keys : List Node)
    (cl : ConsistencyLevel) : Bool :=
  pats.all (fun l => keys.any (fun k => !(t.okAtB (downOf l) k cl)))

/-- `true` iff every failure pattern in `pats` leaves `cl` available for every key. -/
def Topo.survivesAll (t : Topo) (pats : List (List Node)) (keys : List Node)
    (cl : ConsistencyLevel) : Bool :=
  pats.all (fun l => keys.all (fun k => t.okAtB (downOf l) k cl))

/-- Every ordered pair drawn from `l` satisfies `p`. -/
def pairsAll (l : List (List Node)) (p : List Node → List Node → Bool) : Bool :=
  l.all (fun a => l.all (fun b => p a b))

/-- `true` iff two quorum-sized ack sets from `l` can be disjoint. -/
def someDisjointPair (l : List (List Node)) (need : Nat) : Bool :=
  l.any (fun a => l.any (fun b => Nat.ble need a.length && Nat.ble need b.length && mayMiss a b))

end Ferrosa.Topology
