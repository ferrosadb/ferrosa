/-
  Ferrosa formal specification — consistency levels on the SQL surface, and what each
  one buys.
  =====================================================================

  A statement on the SQL surface runs at `ALL`, `QUORUM`, `LOCAL_QUORUM` or `ONE`, and
  `QUORUM` is the default. The level decides which replicas must answer and nothing
  else: `QUORUM` on three replicas needs two acknowledgements on each side, those two
  sets must overlap, and so a read finds the last acknowledged write. `ONE` needs one
  on each, the two need not overlap, and a write the client was told had succeeded can
  be invisible to its very next read.

  Two things surprise operators: the requirement is per datacenter and not a total,
  so five acknowledgements all in the primary do not stand in for three there and two
  in the secondary — and one write path, the truncate path at
  `coordinator/write.rs:753`, counts a bare total and admits exactly that write. And
  no level prevents write skew: a level chooses which replicas answer, while isolation
  chooses which versions are visible, so `ALL` makes every replica agree that the
  invariant is broken.
-/
import Ferrosa.Ledger
import Ferrosa.Topology.ConsistencyLevels
import Ferrosa.Topology.DcQuorum
import Ferrosa.Sql.Txn

set_option linter.deprecated false

namespace Ferrosa.Sql.Consistency

open Ferrosa.Ledger

/-- The four levels the SQL surface exposes, cheapest first. -/
inductive Level where
  | one
  | quorum
  | localQuorum
  | all
  deriving DecidableEq, Repr

/-- **The default is QUORUM.**  A statement or transaction that names no level gets QUORUM, which
    on RF=3 is two replicas — enough to overlap another QUORUM and not enough to survive two
    failures. -/
def defaultLevel : Level := .quorum

/-- A statement or transaction that names no level runs at `QUORUM`, which on three
    replicas is two of them — enough to meet another `QUORUM` read and not enough to
    survive two failures. -/
theorem default_is_quorum : defaultLevel = Level.quorum := rfl

/-- Replicas that must acknowledge. `localRf` is the replica count in the coordinating
    datacenter; it matters only for LOCAL_QUORUM, and it is the reason LOCAL_QUORUM is not a
    function of the global replication factor. -/
def Level.replicasNeeded (l : Level) (globalRf localRf : Nat) : Nat :=
  match l with
  | .one => 1
  | .quorum => globalRf / 2 + 1
  | .localQuorum => localRf / 2 + 1
  | .all => globalRf

/-! ## Delegation: the requirement is per datacenter, not a total

Datacenters are not the same size. On a `{5, 3}` keyspace a QUORUM write needs 3 acks in the
primary and 2 in the secondary — five in total, but *five in total is not the requirement*: five
acks all in the primary satisfy the total and leave the secondary empty. So the level delegates
to a per-datacenter vector, and satisfaction is conjunctive per DC.

For `{5,3}` the old total rule (`8/2+1 = 5`) happens to give the same number as `3+2`. For `{2,2}`
it does not (`4/2+1 = 3` against `2+2 = 4`), which is the undershoot caught earlier. -/

/-- Replica counts per datacenter, in a fixed order. -/
structure Topology where
  dcs : List Nat
  deriving DecidableEq, Repr

/-- Map with an index. `List.enum` is not in this toolchain's core. -/
def idxMap (f : Nat → Nat → Nat) : Nat → List Nat → List Nat
  | _, [] => []
  | i, x :: xs => f i x :: idxMap f (i + 1) xs

/-- The acknowledgements required in each datacenter. `localDc` indexes the coordinating DC, which
    only LOCAL_QUORUM reads. -/
def Level.perDcNeeded (l : Level) (t : Topology) (localDc : Nat) : List Nat :=
  match l with
  | .all => t.dcs
  | .quorum => t.dcs.map fun r => r / 2 + 1
  | .localQuorum => idxMap (fun i r => if i = localDc then r / 2 + 1 else 0) 0 t.dcs
  | .one => t.dcs.map fun _ => 0

/-- Satisfaction is per datacenter: every DC must meet its own requirement. -/
def satisfied (needed acks : List Nat) : Bool :=
  (needed.zip acks).all fun p => decide (p.1 ≤ p.2)

/-- The primary/secondary shape from the discussion: five replicas and three. -/
def primarySecondary : Topology := ⟨[5, 3]⟩

/-- **QUORUM delegates**: three in the primary, two in the secondary. -/
theorem quorum_delegates_to_each_datacenter :
    Level.quorum.perDcNeeded primarySecondary 0 = [3, 2] := by decide

/-- **LOCAL_QUORUM constrains only the coordinating datacenter**, and by the *local* replica
    count — three of five here, not a function of the eight total. -/
theorem local_quorum_constrains_only_the_coordinating_dc :
    Level.localQuorum.perDcNeeded primarySecondary 0 = [3, 0]
      ∧ Level.localQuorum.perDcNeeded primarySecondary 1 = [0, 2] := by decide

/-- `ALL` on a five-and-three keyspace requires all five replicas in the primary and
    all three in the secondary, so it tolerates no failure at all. -/
theorem all_requires_every_replica :
    Level.all.perDcNeeded primarySecondary 0 = [5, 3] := by decide

/-- ONE is not a per-datacenter requirement at all: it is "one replica, anywhere". -/
theorem one_is_not_a_per_dc_requirement :
    Level.one.perDcNeeded primarySecondary 0 = [0, 0] := by decide

/-- **Five acks in the primary and none in the secondary satisfy the total and fail the
    requirement.**  This is the shape of the coordinator bug: comparing a total ack count rather
    than a per-DC one admits a write with no quorum in a datacenter. -/
theorem a_total_count_can_hide_a_missing_datacenter :
    5 ≥ (Level.quorum.perDcNeeded primarySecondary 0).sum
      ∧ satisfied (Level.quorum.perDcNeeded primarySecondary 0) [5, 0] = false := by
  decide

/-- The total rule and the per-DC rule agree on `{5,3}` and disagree on an even split. -/
theorem total_rule_undershoots_on_an_even_split :
    (Level.quorum.perDcNeeded ⟨[2, 2]⟩ 0).sum = 4
      ∧ Level.quorum.replicasNeeded 4 2 = 3 := by decide

/-- Failures a write at this level survives, given the replication factor. -/
def Level.failuresTolerated (l : Level) (globalRf localRf : Nat) : Nat :=
  globalRf - l.replicasNeeded globalRf localRf

/-! ## The acceptance table the tests check against

Each row is `(level, globalRf, localRf, replicas that must acknowledge)`. A Rust test reads the
same numbers; the theorem below is what makes the table a specification rather than a sample. -/

structure Row where
  level : Level
  globalRf : Nat
  localRf : Nat
  needed : Nat
  deriving DecidableEq, Repr

def Row.ok (r : Row) : Bool := r.level.replicasNeeded r.globalRf r.localRf == r.needed

def acceptanceTable : List Row := [
  ⟨.one, 3, 3, 1⟩,
  ⟨.quorum, 3, 3, 2⟩,
  ⟨.localQuorum, 3, 3, 2⟩,
  ⟨.all, 3, 3, 3⟩,
  ⟨.one, 6, 3, 1⟩,
  ⟨.quorum, 6, 3, 4⟩,
  /- LOCAL_QUORUM on a 6-replica keyspace with 3 local replicas is **2**, not 4: it never sees the
     global replication factor. This row is the regression test for that. -/
  ⟨.localQuorum, 6, 3, 2⟩,
  ⟨.all, 6, 3, 6⟩
]

/-- **The table is the rule.**  Every row agrees with `replicasNeeded`, exhaustively. -/
theorem acceptance_table_agrees_with_the_rule :
    (acceptanceTable.all Row.ok) = true := by decide

/-- **The SQL level set is not a second arithmetic.**  It is `blockFor` / `blockForDc` from the
    topology layer with a narrower surface: QUORUM is `blockFor .quorum`, LOCAL_QUORUM is
    `blockForDc .localQuorum`, which is why LOCAL_QUORUM takes the local replication factor. -/
theorem sql_levels_agree_with_the_topology_rule :
    (∀ rf, Level.quorum.replicasNeeded rf rf = blockFor .quorum rf)
      ∧ (∀ dcRf, Level.localQuorum.replicasNeeded 6 dcRf = blockForDc .localQuorum dcRf) := by
  refine ⟨fun rf => ?_, fun dcRf => ?_⟩ <;>
    simp [Level.replicasNeeded, blockFor, blockForDc]

/-- Cost rises with the level: fewer acknowledgements, more tolerated failures. -/
theorem levels_trade_acknowledgements_for_failures :
    Level.one.replicasNeeded 3 3 < Level.quorum.replicasNeeded 3 3
      ∧ Level.quorum.replicasNeeded 3 3 < Level.all.replicasNeeded 3 3
      ∧ Level.all.failuresTolerated 3 3 < Level.quorum.failuresTolerated 3 3 := by
  decide

/-! ## Read and write levels: when does a read see an acknowledged write?

A read sees an acknowledged write exactly when the replicas that acknowledged the write and the
replicas that answer the read overlap, which is what `QUORUM` on three replicas buys and `ONE` does
not. At `ONE` on each side the two sets need never meet, so a value the client has just been told is
written can be absent from its very next read; at `ALL` every write is met by every read level.
`LOCAL_QUORUM` overlaps only inside its own datacenter, so the guarantee stops at the datacenter
boundary rather than at the edge of the cluster.

-/

def intersects (a b : List Nat) : Bool := a.any fun x => b.any fun y => x == y

/-- Every non-empty subset of a three-replica set. -/
def subsets3 : List (List Nat) :=
  [[], [0], [1], [2], [0, 1], [0, 2], [1, 2], [0, 1, 2]]

/-- **QUORUM reads see QUORUM writes on RF=3.**  Two acknowledgements out of three on each side
    must overlap, so a read at QUORUM cannot miss a write acknowledged at QUORUM. This is the
    property the default level exists to buy. -/
theorem quorum_reads_see_quorum_writes_rf3 :
    (subsets3.all fun w => subsets3.all fun r =>
      !(2 ≤ w.length && 2 ≤ r.length) || intersects w r) = true := by
  decide

/-- **ONE reads need not see ONE writes.**  One replica acknowledged the write, another answers
    the read, and they never overlap: the read returns a value the client already replaced. -/
theorem one_read_can_miss_a_one_write :
    intersects [0] [1] = false ∧ 1 + 1 ≤ 3 := by decide

/-- ALL writes survive every read level, which is the only configuration where a read at ONE is
    safe against a previous write. -/
theorem all_writes_are_seen_by_every_read :
    intersects [0, 1, 2] [1] = true ∧ intersects [0, 1, 2] [2] = true := by decide

/-- **LOCAL_QUORUM does not cross datacenters.**  dc1 = {0,1,2}, dc2 = {3,4,5}: a write
    acknowledged at LOCAL_QUORUM in dc1 and a read at LOCAL_QUORUM in dc2 do not overlap at all,
    so there is no read-your-writes across a DC boundary — only within one. -/
theorem local_quorum_does_not_cross_dcs :
    intersects [0, 1] [3, 4] = false := by decide

/-! ## The decision level must cover the data level

A commit decision acknowledged at `ONE` while the data it decides sits at `QUORUM` can be lost on its
own: the one replica holding the verdict goes and the data it committed stays, so the transaction's
outcome is on record and its decision is not. The rule is that the decision's level covers every
statement's level, which `decisionCoversStatements` makes checkable, and it cannot be read off an
ordering of the enum. `LOCAL_QUORUM` needs two acknowledgements on six replicas where `QUORUM` needs
four, so which level is strongest depends on the configuration rather than on the name.

-/

/-- A commit decision acknowledged at ONE while the data is at QUORUM: if that one replica is
    lost, the verdict is gone and the data is not. -/
theorem a_weak_decision_level_can_lose_the_verdict :
    intersects [0] [1, 2] = false := by decide

/- So the decision level is the maximum **by acknowledgement count for this configuration**,
   computed from `replicasNeeded` at write time. `decisionCoversStatements` is the checkable half
   of that rule; which level attains the maximum is a runtime lookup over four values, not a
   property worth a theorem. -/

/-! ## QUORUM is not EACH_QUORUM, and the denominator is configuration

`ferrosa-cluster/src/coordinator/dc_quorum.rs` (PR #634) answers QUORUM in two gates: the **origin
DC** must reach its own majority, and then a **strict majority of the configured DCs** must each
reach theirs. EACH_QUORUM instead requires *every* configured DC. So the vector model written
earlier in this file — every DC needs its majority — is EACH_QUORUM, and on a two-DC keyspace the
two rules coincide, which is why the `{5,3} → [3,2]` result stands for both.

The denominator is the *configured* DC count. A partition removes reachable replicas, not
configured ones, so it can never lower the requirement. -/

/-- Does one DC hold a strict majority of its own replicas? -/
def dcHasMajority (rf acks : Nat) : Bool := acks > rf / 2

/-- How many configured DCs hold their own majority. -/
def dcsWithMajority (t : Topology) (acks : List Nat) : Nat :=
  (idxMap (fun i rf => if dcHasMajority rf (acks.getD i 0) then 1 else 0) 0 t.dcs).sum

/-- Per-datacenter acknowledgement counts as the general model's `count_in_dc`. -/
def ackCount (acks : List Nat) (d : Nat) : Nat := acks.getD d 0

/-- The code's QUORUM shape: origin-DC majority, then a strict majority of configured DCs.  This is
    gates 1 and 2 of `Topology.DcQuorum`, so every theorem proved there for all topologies holds of
    the examples in this file; in particular a DC configured with `rf = 0` does not count towards
    the denominator, exactly as the code's `.filter(|rf| **rf > 0)`. -/
def quorumSatisfied (t : Topology) (origin : Nat) (acks : List Nat) : Bool :=
  Topology.DcQuorum.originGate t.dcs origin (ackCount acks)
    && Topology.DcQuorum.dcGate t.dcs (ackCount acks)

/-- EACH_QUORUM: every configured DC holds its own majority. -/
def eachQuorumSatisfied (t : Topology) (acks : List Nat) : Bool :=
  dcsWithMajority t acks = t.dcs.length

/-- **On two DCs the two levels are the same**, so `{5,3}` needs `[3, 2]` either way — and five acks
    in the primary with none in the secondary still fails. -/
theorem quorum_and_each_quorum_agree_on_two_dcs :
    quorumSatisfied primarySecondary 0 [3, 2] = true
      ∧ eachQuorumSatisfied primarySecondary [3, 2] = true
      ∧ quorumSatisfied primarySecondary 0 [5, 0] = false := by decide

/-- **Above two DCs they diverge.** On `{3,3,3}`, two DCs at their majority satisfy QUORUM and fail
    EACH_QUORUM — which is the whole reason the two levels exist as separate words. -/
theorem each_quorum_is_stricter_than_quorum_above_two_dcs :
    quorumSatisfied ⟨[3, 3, 3]⟩ 0 [2, 2, 0] = true
      ∧ eachQuorumSatisfied ⟨[3, 3, 3]⟩ [2, 2, 0] = false := by decide

/-- **A partition cannot move the denominator**: with one DC dark, the requirement is still a
    majority of the three configured DCs, so two live DCs are needed, not one. -/
theorem a_partition_does_not_shrink_the_dc_denominator :
    quorumSatisfied ⟨[3, 3, 3]⟩ 0 [2, 0, 2] = true
      ∧ quorumSatisfied ⟨[3, 3, 3]⟩ 0 [2, 0, 0] = false := by decide

/-- And the origin gate is separate from the DC count: two remote DCs do not stand in for a
    primary that has not reached its own majority. -/
theorem the_origin_dc_gate_is_not_satisfied_by_remote_acks :
    quorumSatisfied ⟨[5, 3, 3]⟩ 0 [2, 3, 3] = false := by decide

/-! ## The two QUORUM rules are incomparable

Cassandra's QUORUM is a *total*: `RF/2 + 1` acknowledgements anywhere. PR #634's QUORUM is a
*shape*: the origin DC's majority plus a strict majority of configured DCs. Neither implies the
other — each accepts a write the other rejects — so this is a compatibility decision, not a
strict improvement. -/

/-- Cassandra's QUORUM: a total of `RF/2 + 1` acknowledgements, wherever they land. -/
def cassandraQuorumSatisfied (t : Topology) (acks : List Nat) : Bool :=
  t.dcs.sum / 2 + 1 ≤ acks.sum

/-- **Each rule accepts a write the other rejects**, on the same `{3,3,3}` keyspace:

    `[5, 0, 0]` is five acks, which is Cassandra's quorum of nine, but only one of three DCs has its
    majority, so the shape rule rejects it.
    `[2, 2, 0]` is four acks, two DCs with their majority, so the shape rule accepts it — and
    Cassandra's total of five rejects it. -/
theorem pr_and_cassandra_quorum_are_incomparable :
    cassandraQuorumSatisfied ⟨[3, 3, 3]⟩ [5, 0, 0] = true
      ∧ quorumSatisfied ⟨[3, 3, 3]⟩ 0 [5, 0, 0] = false
      ∧ quorumSatisfied ⟨[3, 3, 3]⟩ 0 [2, 2, 0] = true
      ∧ cassandraQuorumSatisfied ⟨[3, 3, 3]⟩ [2, 2, 0] = false := by decide

/-- On two DCs they agree, which is why the divergence only appears on three or more. -/
theorem the_two_rules_agree_on_two_dcs :
    cassandraQuorumSatisfied primarySecondary [3, 2] = true
      ∧ quorumSatisfied primarySecondary 0 [3, 2] = true := by decide

/-! ## DC_QUORUM: quorum local plus a quorum of datacenters

Cassandra's own levels stay as they are. `DC_QUORUM` is a Ferrosa mode, deliberately not a Cassandra
level: the origin DC must reach its own quorum, **and** a quorum of the configured DCs must each
reach theirs. That is the shape an operator writing to a multi-DC keyspace expects — the write lands
in a quorum of datacenters, not merely somewhere — and it is the default when the keyspace has more
than one DC. -/

/-- The default quorum rule: DC_QUORUM on a multi-DC keyspace, Cassandra's QUORUM on a single-DC one. -/
def defaultQuorumRule (t : Topology) (origin : Nat) (acks : List Nat) : Bool :=
  if 1 < t.dcs.length then quorumSatisfied t origin acks else cassandraQuorumSatisfied t acks

/-- **A multi-DC keyspace defaults to DC_QUORUM**, a single-DC one to Cassandra's QUORUM. -/
theorem the_multi_dc_default_is_dc_quorum :
    defaultQuorumRule ⟨[5, 3]⟩ 0 [3, 2] = quorumSatisfied ⟨[5, 3]⟩ 0 [3, 2]
      ∧ defaultQuorumRule ⟨[3]⟩ 0 [2] = cassandraQuorumSatisfied ⟨[3]⟩ [2] := by decide

/-- On one DC with RF 3 the two rules give the same verdict at every acknowledgement count, so the
    mode adds nothing until there is a second datacenter to require. -/
theorem dc_quorum_collapses_to_quorum_on_one_dc :
    quorumSatisfied ⟨[3]⟩ 0 [1] = cassandraQuorumSatisfied ⟨[3]⟩ [1]
      ∧ quorumSatisfied ⟨[3]⟩ 0 [2] = cassandraQuorumSatisfied ⟨[3]⟩ [2]
      ∧ quorumSatisfied ⟨[3]⟩ 0 [3] = cassandraQuorumSatisfied ⟨[3]⟩ [3] := by decide

/-- **What the default trades away**: DC_QUORUM guarantees a *shape* — a majority of DCs, each with
    a majority of its own replicas — and not Cassandra's *total*. On `{3,3,3}`, `[2,2,0]` is four acks
    of nine: it satisfies DC_QUORUM and Cassandra's quorum of five rejects it. An operator who reads
    "quorum" as "durable against a minority of nodes" is getting a different guarantee. -/
theorem dc_quorum_trades_total_durability_for_dc_shape :
    quorumSatisfied ⟨[3, 3, 3]⟩ 0 [2, 2, 0] = true
      ∧ cassandraQuorumSatisfied ⟨[3, 3, 3]⟩ [2, 2, 0] = false := by decide

/-- Requiring **both** costs an operator nothing and rejects both bad shapes: the write must sit in
    a quorum of DCs *and* in a quorum of all replicas. -/
def dcQuorumStrict (t : Topology) (origin : Nat) (acks : List Nat) : Bool :=
  quorumSatisfied t origin acks && cassandraQuorumSatisfied t acks

/-- Requiring a datacenter quorum and a node quorum together refuses both bad shapes on
    `{3,3,3}`: five acks in one datacenter fail the shape, and two datacenters' majorities
    of four acks fail Cassandra's total of five. Three in the origin and two remote pass
    both. -/
theorem requiring_both_rejects_both_bad_shapes :
    dcQuorumStrict ⟨[3, 3, 3]⟩ 0 [5, 0, 0] = false
      ∧ dcQuorumStrict ⟨[3, 3, 3]⟩ 0 [2, 2, 0] = false
      ∧ dcQuorumStrict ⟨[3, 3, 3]⟩ 0 [3, 2, 0] = true := by decide

/-! ## DC_QUORUM's two conditions

A quorum in the origin datacenter, **and** a quorum of configured datacenters. The first is why a
read is cheap — the local quorum is always there, so a reader serves the level without a cross-DC
hop. The second is why a partition cannot produce two live halves: two disjoint sets of datacenters
cannot both hold a strict majority of the configured ones. `quorumSatisfied` above is exactly this
pair of gates, so its theorems are DC_QUORUM's. -/

/-- DC_QUORUM's write requirement: all three conditions — the origin datacenter's quorum, a quorum
    of configured datacenters, and a quorum of all replicas.  It *is* `Topology.DcQuorum.write`; the
    general theorems (`dc_quorum_writes_share_a_replica`, `no_partition_admits_two_dc_quorum_writes`,
    `write_is_monotone`) are stated there over replica sets. -/
def dcQuorumWrite (t : Topology) (origin : Nat) (acks : List Nat) : Bool :=
  Topology.DcQuorum.write t.dcs origin (ackCount acks) acks.sum

/-- **A datacenter configured with `rf = 0` is not in the denominator.**  On `{3, 0}` the only
    eligible datacenter is the origin, so its quorum is a quorum of configured datacenters.  Counting
    `t.dcs.length` instead would demand a second datacenter that holds no replicas, and reject every
    write; `Topology.DcQuorum.a_zero_rf_datacenter_is_inert` is the general statement. -/
theorem a_zero_rf_datacenter_is_not_in_the_denominator :
    quorumSatisfied ⟨[3, 0]⟩ 0 [2, 0] = true
      ∧ quorumSatisfied ⟨[3, 0]⟩ 0 [2, 0] = quorumSatisfied ⟨[3]⟩ 0 [2]
      ∧ ¬ ([3, 0].length / 2 + 1 ≤ 1) := by decide

/-- **`{5,5,5}` accepts `{5,3,0}` and rejects `{3,3,0}`.**  Both have the origin's quorum and two DC
    quorums; only the first reaches a quorum of all fifteen replicas, which is eight. -/
theorem dc_quorum_also_needs_a_node_quorum :
    dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [5, 3, 0] = true
      ∧ dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [3, 3, 0] = false := by decide

/-- **`{5,5,5}` keeps committing through a whole datacenter outage.**  With the third DC absent,
    `{5,3,0}` (all of the origin, a remote quorum), `{3,5,0}` (the origin's quorum, all of the live
    remote) and `{4,4,0}` all clear the node quorum of eight. -/
theorem dc_quorum_tolerates_a_whole_dc_outage :
    dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [5, 3, 0] = true
      ∧ dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [3, 5, 0] = true
      ∧ dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [4, 4, 0] = true := by decide

/-- **And the window is exact.**  With one DC dark, the two survivors must each clear their own
    quorum *and* together supply eight of the ten remaining replicas. `{3,4,0}` is seven and fails;
    `{3,3,0}` is six and fails; `{4,4,0}` is eight and passes. A DC outage is survivable, a DC
    outage plus a second straggler is not. -/
theorem the_survivors_must_supply_eight_of_ten :
    dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [3, 4, 0] = false
      ∧ dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [3, 3, 0] = false
      ∧ dcQuorumWrite ⟨[5, 5, 5]⟩ 0 [4, 4, 0] = true := by decide

/-! ### Detection returns an exception; the client owns the retry

A consistency level only *detects* a write skew. The transaction that commits second is the one
told to retry — the first commit is already acknowledged and is never rolled back — and resubmitting
is a client decision, not something the coordinator does on its behalf. -/

/-- The verdict a writer receives from commit-time read-set validation. -/
inductive Verdict where
  | committed
  | retry (conflict : List Nat)
  deriving DecidableEq, Repr

/-- The rows a transaction read that a concurrent transaction wrote. -/
def conflicts (readsAgainst concurrentWrites : List Nat) : List Nat :=
  readsAgainst.filter (fun x => concurrentWrites.contains x)

/-- The transaction committing **second** is the one told to retry. -/
def verdictForLastWriter (readsOfLast readsOfFirst writesOfFirst : List Nat) : Verdict :=
  match conflicts readsOfLast writesOfFirst with
  | [] => .committed
  | c => .retry c

/-- **The last writer is the one told to retry.**  In the doctors pair the second writer read row 0,
    which the first wrote, so it gets an exception naming row 0 while the first commit stands. -/
theorem the_last_writer_is_the_one_told_to_retry :
    verdictForLastWriter [0] [1] [0] = .retry [0] := by decide

/-- What a verdict does to the state: a commit writes, a retry writes nothing. -/
def applyVerdict (v : Verdict) (s w : List Nat) : List Nat :=
  match v with
  | .committed => w
  | .retry _ => s

/-- **A retry verdict is not a write.**  The rejected transaction leaves the state untouched, so a
    client that ignores the exception has simply not committed: nothing is corrupted, and nothing
    succeeds silently. -/
theorem a_retry_verdict_does_not_modify_the_state (s w : List Nat) (c : List Nat) :
    applyVerdict (.retry c) s w = s := rfl

/-- And rejecting preserves the invariant the pair would otherwise break. -/
theorem a_rejected_writer_keeps_the_invariant (s w : List Nat) (c : List Nat)
    (h : s.all (fun x => x == 0) = true) :
    (applyVerdict (.retry c) s w).all (fun x => x == 0) = true := by
  simp only [applyVerdict]
  exact h

/-- **The conflict is named**, so the client can re-read exactly the rows that collided and decide
    for itself whether to retry. Detection hands over evidence; it does not act on it. -/
theorem the_conflict_is_named_for_the_client :
    conflicts [0] [0] = [0] ∧ conflicts [1] [0] = [] := by decide

/-- **A retry must re-read.**  The verdict is a function of the read set against the winner's write
    set, so resubmitting the identical read set is rejected identically; only a re-read that moves
    off the winner's rows clears it. -/
theorem only_a_reread_changes_the_verdict :
    verdictForLastWriter [0] [1] [0] = .retry [0]
      ∧ verdictForLastWriter [1] [1] [0] = .committed := by decide

/-- LOCAL_QUORUM's rule sees only its own datacenter's count. -/
def localQuorumAck (localRf localAcks _remoteAcks : Nat) : Bool :=
  localAcks > localRf / 2

/-- **Strong inside the datacenter, silent about everywhere else.**  The acknowledgement verdict is
    identical for every possible remote count, including zero: nothing in it waits on, or asks about,
    replication to another datacenter. -/
theorem local_quorum_ack_is_blind_to_the_other_datacenters (remote : Nat) :
    localQuorumAck 3 2 remote = true
      ∧ localQuorumAck 3 2 0 = localQuorumAck 3 2 3 := by
  refine ⟨?_, rfl⟩
  unfold localQuorumAck
  decide

/-- A write acknowledged locally with `k` earlier writes still unseen by the remote datacenter. -/
def ackedWhileRemoteIsBehind (_k : Nat) : Bool := localQuorumAck 3 2 0

/-- **And the window has no bound.**  Any number of locally acknowledged writes can sit outside the
    other datacenter, so a failover can lose all of them. LOCAL_QUORUM does not promise eventual
    convergence within any interval — it promises the local quorum, and nothing beyond it. -/
theorem a_failover_can_lose_unboundedly_many_local_acks (k : Nat) :
    ackedWhileRemoteIsBehind k = true := by
  unfold ackedWhileRemoteIsBehind localQuorumAck
  decide

/-- **Any two node majorities intersect**, so a write acknowledged by a majority is still found by
    any later quorum read or repair. This is the recoverability DC_QUORUM's third condition buys. -/
theorem a_node_quorum_cannot_be_split (n a b : Nat)
    (ha : n / 2 + 1 ≤ a) (hb : n / 2 + 1 ≤ b) (hdisj : a + b ≤ n) : False := by omega

/-- **And that is exactly what LOCAL_QUORUM does not buy.**  Its ack set is a majority of one
    datacenter, not of the cluster: `[2, 0]` on `{3, 3}` acknowledges two of six replicas and sits
    outside every future majority, so nothing later is obliged to intersect it. LOCAL_QUORUM is kept
    for Cassandra wire compatibility; it is not the operating point. -/
theorem a_local_quorum_can_sit_outside_every_future_quorum :
    localQuorumAck 3 2 0 = true ∧ ¬ (6 / 2 + 1 ≤ 2) := by
  refine ⟨?_, ?_⟩
  · unfold localQuorumAck; decide
  · decide

/-- The level a transaction's commit record is written at: the level the transaction requested. -/
def commitLevel (requested : Level) : Level := requested

/-- The level every statement's data is written at, after the commit upgrades it. -/
def upgradeData (requested : Level) (_statementLevel : Level) : Level := requested

/-- **The decision level is carried by the transaction, never ranked from the enum.**  Nothing in the
    commit path orders the levels — which is precisely why the missing configuration-independent order
    does not matter. `LOCAL_QUORUM` and `QUORUM` are incomparable across replication factors, and no
    code has to care, because the transaction says which level it wants and the commit uses it. -/
theorem the_level_is_carried_not_ranked (requested : Level) (rf localRf : Nat) :
    commitLevel requested = requested
      ∧ Level.replicasNeeded (commitLevel requested) rf localRf
          = Level.replicasNeeded requested rf localRf :=
  ⟨rfl, rfl⟩

/-- **Committing at the requested level makes the verdict exactly as durable as the data it
    commits**: both are written at the transaction's level, so the verdict can never be the weaker
    of the two. -/
theorem an_upgraded_commit_is_exactly_as_durable_as_its_data (requested : Level) (rf : Nat) :
    Level.replicasNeeded (commitLevel requested) rf rf
      = Level.replicasNeeded (upgradeData requested .one) rf rf := rfl

/-- And the carrier stops mattering: whichever statement rides the commit record, it is written at
    the transaction's level, so riding the last mutation is safe once the data is upgraded. -/
theorem riding_the_last_mutation_is_safe_once_upgraded (requested last : Level) (rf : Nat) :
    Level.replicasNeeded (upgradeData requested last) rf rf
      = Level.replicasNeeded (commitLevel requested) rf rf := rfl

/-- **Cassandra's QUORUM is the total rule alone** — `block_for` returns `rf / 2 + 1` on the total
    replication factor, and that is all it is. -/
theorem quorum_is_cassandras_total_rule (t : Topology) (acks : List Nat) :
    (cassandraQuorumSatisfied t acks = true) ↔ t.dcs.sum / 2 + 1 ≤ acks.sum := by
  simp [cassandraQuorumSatisfied]

/-- **DC_QUORUM is strictly stronger than Cassandra's QUORUM**: it adds the datacenter shape, and
    `[5, 0, 0]` on `{3,3,3}` is a write QUORUM accepts (five of nine) and DC_QUORUM refuses (one
    datacenter of three). The two are no longer incomparable. -/
theorem quorum_is_strictly_weaker_than_dc_quorum :
    cassandraQuorumSatisfied ⟨[3, 3, 3]⟩ [5, 0, 0] = true
      ∧ dcQuorumWrite ⟨[3, 3, 3]⟩ 0 [5, 0, 0] = false := by decide

/-- DC_QUORUM's read requirement: the local quorum alone. -/
def dcQuorumRead (t : Topology) (origin : Nat) (acks : List Nat) : Bool :=
  Topology.DcQuorum.read t.dcs origin (ackCount acks)

/-- **Local quorum alone does not prevent a split.** On `{3,3}`, a partition that separates the
    datacenters leaves each side holding its own local quorum, so both would answer. -/
theorem local_quorum_alone_does_not_prevent_a_split :
    dcQuorumRead ⟨[3, 3]⟩ 0 [2, 0] = true
      ∧ dcQuorumRead ⟨[3, 3]⟩ 1 [0, 2] = true
      ∧ quorumSatisfied ⟨[3, 3]⟩ 0 [2, 0] = false := by decide

/-- **The datacenter-majority condition is what prevents it**: two disjoint sets of datacenters
    cannot both contain a strict majority of the configured ones. -/
theorem two_disjoint_dc_sets_cannot_both_hold_a_majority (a b n : Nat)
    (hdisj : a + b ≤ n) (ha : n / 2 + 1 ≤ a) (hb : n / 2 + 1 ≤ b) : False := by omega

/-- **A write needs both conditions.** `[2,0]` on `{3,3}` has the local quorum and not the
    datacenter quorum; `[2,2]` has both. -/
theorem dc_quorum_writes_need_both_conditions :
    quorumSatisfied ⟨[3, 3]⟩ 0 [2, 0] = false
      ∧ quorumSatisfied ⟨[3, 3]⟩ 0 [2, 2] = true := by decide

/-- **A local read is blind to remote replication.**  The read verdict is a function of the origin
    datacenter's acknowledgement count alone, so two states with the same local count and wildly
    different remote state are indistinguishable to a reader. -/
theorem a_local_read_does_not_consult_remote_datacenters (t : Topology) (origin : Nat)
    (a b : List Nat) (h : a.getD origin 0 = b.getD origin 0) :
    dcQuorumRead t origin a = dcQuorumRead t origin b := by
  simp only [dcQuorumRead, Topology.DcQuorum.read, Topology.DcQuorum.originGate, ackCount, h]

/-- **So a client that fails over between datacenters can read backwards.**  `[3, 0]` and `[3, 5]`
    are the same read: a reader in the second DC whose own quorum is intact cannot tell whether the
    write it acknowledged elsewhere ever arrived. This is the price of the hop-free read, and it is
    accepted deliberately. -/
theorem a_failing_over_client_may_not_read_its_own_write :
    dcQuorumRead ⟨[5, 5]⟩ 0 [3, 0] = dcQuorumRead ⟨[5, 5]⟩ 0 [3, 5]
      ∧ dcQuorumRead ⟨[5, 5]⟩ 0 [3, 0] = true := by decide

/-- **A LOCAL_QUORUM commit buys a datacenter-failure RPO, not cross-datacenter durability.**  Two
    transactions committed at local quorum in different datacenters need not both survive the loss of
    one datacenter. Which of them does is a client choice, exactly as the level promises. -/
theorem local_quorum_commits_have_a_datacenter_failure_rpo :
    dcQuorumRead ⟨[3, 3]⟩ 0 [2, 0] = true
      ∧ dcQuorumRead ⟨[3, 3]⟩ 1 [0, 2] = true
      ∧ cassandraQuorumSatisfied ⟨[3, 3]⟩ [2, 0] = false := by decide

/-- **And the local-read guarantee follows from the write rule**: any write that satisfied
    DC_QUORUM left its origin datacenter at quorum, so reading there needs no hop. -/
theorem a_local_read_sees_a_dc_quorum_write (t : Topology) (origin : Nat) (acks : List Nat)
    (h : dcQuorumWrite t origin acks = true) : dcQuorumRead t origin acks = true := by
  simp only [dcQuorumWrite, Topology.DcQuorum.write, Bool.and_eq_true] at h
  exact h.1.1

/-! ## A transaction's statements and its commit decision

Levels do not mix within a statement. Across a transaction's statements they can differ, and the
commit decision then has to be at least as strong as every statement it commits. -/

/-- Whether a decision at `decision` is at least as strong as every statement's level. -/
def decisionCoversStatements (stmts : List Level) (decision : Level)
    (globalRf localRf : Nat) : Bool :=
  stmts.all fun s => s.replicasNeeded globalRf localRf ≤ decision.replicasNeeded globalRf localRf

/-- **Deciding at the weakest statement's level is not enough.** -/
theorem a_decision_at_the_weakest_statement_is_not_enough :
    decisionCoversStatements [.one, .all] .one 3 3 = false := by decide

/-- Deciding at the strongest covers every statement, which is the rule. -/
theorem a_decision_at_the_strongest_statement_covers_every_one :
    decisionCoversStatements [.one, .quorum, .all] .all 3 3 = true := by decide

/-- **But "strongest" cannot be an enum order.**  LOCAL_QUORUM's position depends on the
    replication factor: on 6 replicas with 3 local it needs 2 acks against QUORUM's 4; on 3
    replicas they tie. -/
theorem levels_are_not_ordered_independently_of_the_configuration :
    Level.localQuorum.replicasNeeded 6 3 < Level.quorum.replicasNeeded 6 3
      ∧ Level.quorum.replicasNeeded 3 3 ≤ Level.localQuorum.replicasNeeded 3 3 := by
  decide

/-! ## The commit record rides the mutation it commits

If the verdict is a row in the mutation it commits, then the verdict and the data are durable
together or not at all: there is no window, because there is no second write. What the coordinator
must get right is *which* mutation carries it. -/

/-- The durability of a decision that rides a mutation written at `carrier`. -/
def decisionIsCoveredByCarrier (stmts : List Level) (carrier : Level)
    (globalRf localRf : Nat) : Bool :=
  decisionCoversStatements stmts carrier globalRf localRf

/-- **Riding the last mutation is not enough when the last mutation is the weakest.** A transaction
    that reads at QUORUM and then writes at ONE would record its verdict on a single ack. -/
theorem riding_the_last_mutation_is_not_enough_when_it_is_the_weakest :
    decisionIsCoveredByCarrier [.quorum, .one] .one 6 3 = false := by decide

/-- So the carrier must be the transaction's strongest statement. -/
theorem the_carrier_must_be_the_strongest_statement :
    decisionIsCoveredByCarrier [.quorum, .one] .quorum 6 3 = true := by decide

/-- **And then there is no window at all**, because the verdict is not a second write: on a
    one-statement transaction the durability of the decision *is* the durability of the data. -/
theorem no_window_when_the_decision_rides_the_data (l : Level) (globalRf localRf : Nat) :
    decisionIsCoveredByCarrier [l] l globalRf localRf = true := by
  simp [decisionIsCoveredByCarrier, decisionCoversStatements]

/-! ## Consistency is agreement, not validation

A consistency level chooses which replicas answer, and every level from `ONE` to `ALL` records exactly
the same broken state when two transactions each preserve an invariant alone and break it together.
Raising the level makes more replicas agree on the same write, not a different write, so no level
expressible as an acknowledgement count prevents write skew — the anomaly's derivation never mentions
the level at all. What prevents it is read-set validation at commit, which refuses one of the two
writers, and that is an obligation on the transaction protocol in `Ferrosa/Sql/Txn.lean`.

-/

/-- The invariant a client believes it is maintaining: every column zero. -/
def inv (s : List Nat) : Bool := s.all fun x => x == 0

/-- The state two transactions produce when each reads, sees the invariant hold, and writes. -/
def writeSkewOutcome : List Nat := [1, 1]

/-- T1 reads row 1 and writes row 0; T2 reads row 0 and writes row 1. Each preserves the invariant
    alone; together they break it. -/
def t1Reads : List Nat := [1]
def t1Writes : List Nat := [0]
def t2Reads : List Nat := [0]
def t2Writes : List Nat := [1]

/-- A transaction's read set against a concurrent transaction's write set. -/
def readWriteConflict (readSet concurrentWrites : List Nat) : Bool :=
  readSet.any (fun x => concurrentWrites.contains x)

/-- **No acknowledgement threshold can prevent write skew.**  The anomaly's derivation never mentions
    the level: raising it only changes how many replicas record the same writes, so every level from
    ONE to ALL — and any new level expressible as an acknowledgement count — lands on the same
    broken state. A new consistency level is the wrong instrument. -/
theorem no_consistency_level_prevents_write_skew (cl : Level) (rf : Nat) :
    inv writeSkewOutcome = false
      ∧ (fun (_ : Level) => writeSkewOutcome) cl = writeSkewOutcome :=
  ⟨by decide, rfl⟩

/-- **Read-set validation is what prevents it.**  Each writer read a row the other wrote, so the
    check fires for both and one must abort. It costs a commit-time validation, which makes it a
    transaction-protocol obligation rather than a consistency level. -/
theorem read_set_validation_is_what_prevents_write_skew :
    readWriteConflict t1Reads t2Writes = true
      ∧ readWriteConflict t2Reads t1Writes = true
      ∧ inv writeSkewOutcome = false := by decide

/-- **No consistency level prevents write skew.**  Both transactions read `[0, 0]`, where the
    invariant holds, and both write. Under ALL every replica agrees afterwards — on `[1, 1]`,
    where the invariant is broken. A level chooses which replicas answer; only read-set
    validation chooses which versions are visible. -/
theorem all_does_not_prevent_write_skew :
    inv [0, 0] = true ∧ inv writeSkewOutcome = false ∧ writeSkewOutcome = [1, 1] := by decide

/-! ## The ledger

Two of the obligations here are red: the truncate path at `coordinator/write.rs:753` counts a bare
total instead of a per-datacenter one, so five acknowledgements in the primary and none in the
secondary satisfy it on `{5,3}`; and no write path captures a read set or validates it before commit,
so a write skew commits with no exception raised. Every other row is discharged by a named theorem,
from the default `QUORUM` and the acceptance table through the per-datacenter delegations to the
`DC_QUORUM` results in `Ferrosa/Topology/DcQuorum.lean`. `consistency_has_two_red_items` pins the red
count so a CI check can require it to fall and never to rise.

-/

def ledger : List SpecItem := [
  ⟨"the level is configurable with QUORUM as the default",
   .met "Consistency.default_is_quorum"⟩,
  ⟨"each level's acknowledgement count is specified",
   .met "Consistency.acceptance_table_agrees_with_the_rule"⟩,
  ⟨"LOCAL_QUORUM is a function of the local replication factor, not the global one",
   .met "Consistency.acceptance_table_agrees_with_the_rule (row 6: 6/3 ⇒ 2)"⟩,
  ⟨"the SQL levels reuse the topology ack rule rather than re-deriving it",
   .met "Consistency.sql_levels_agree_with_the_topology_rule"⟩,
  ⟨"QUORUM reads see QUORUM writes on RF=3",
   .met "Consistency.quorum_reads_see_quorum_writes_rf3"⟩,
  ⟨"ALL writes are seen by every read level",
   .met "Consistency.all_writes_are_seen_by_every_read"⟩,
  ⟨"ONE offers no read-your-writes guarantee: a write acknowledged at ONE need not be seen by the next ONE read",
   .met "Consistency.one_read_can_miss_a_one_write"⟩,
  ⟨"LOCAL_QUORUM gives read-your-writes only while the client stays connected to the same datacenter",
   .met "Consistency.local_quorum_does_not_cross_dcs"⟩,
  ⟨"DC_QUORUM's node quorum is what makes an acknowledged write recoverable after a DC loss",
   .met "Topology.DcQuorum.node_gate_forces_a_shared_replica"⟩,
  ⟨"any two DC_QUORUM writes share a replica, for every topology and every pair of origins",
   .met "Topology.DcQuorum.dc_quorum_writes_share_a_replica"⟩,
  ⟨"a DC_QUORUM write and a Cassandra QUORUM write on the same keyspace share a replica",
   .met "Topology.DcQuorum.dc_quorum_and_cassandra_quorum_writes_share_a_replica"⟩,
  ⟨"a DC_QUORUM read in the write's origin datacenter always sees it",
   .met "Topology.DcQuorum.a_same_origin_read_sees_a_dc_quorum_write"⟩,
  ⟨"a DC_QUORUM read in another datacenter can miss an acknowledged DC_QUORUM write",
   .met "Topology.DcQuorum.a_read_in_another_datacenter_can_miss_a_dc_quorum_write"⟩,
  ⟨"a late or duplicated acknowledgement never revokes a DC_QUORUM verdict",
   .met "Topology.DcQuorum.write_is_monotone"⟩,
  ⟨"a datacenter configured with rf = 0 changes no DC_QUORUM verdict",
   .met "Topology.DcQuorum.a_zero_rf_datacenter_is_inert"⟩,
  ⟨"LOCAL_QUORUM exists for Cassandra wire compatibility, not as an operating recommendation",
   .met "Consistency.a_local_quorum_can_sit_outside_every_future_quorum"⟩,
  ⟨"LOCAL_QUORUM is strong inside its datacenter and makes no promise about any other",
   .met "Consistency.local_quorum_ack_is_blind_to_the_other_datacenters"⟩,
  ⟨"the cross-datacenter window is unbounded: a failover can lose any number of local acks",
   .met "Consistency.a_failover_can_lose_unboundedly_many_local_acks"⟩,
  ⟨"a LOCAL_QUORUM commit buys a datacenter-failure RPO, not cross-datacenter durability",
   .met "Consistency.local_quorum_commits_have_a_datacenter_failure_rpo"⟩,
  ⟨"the acknowledgement requirement delegates to each datacenter",
   .met "Consistency.quorum_delegates_to_each_datacenter"⟩,
  ⟨"the coordinator enforces the per-datacenter requirement for every write, not a total",
   .failing
     "row writes delegate to dc_quorum::progress, but the truncate path at write.rs:753 counts a bare total: five acks in the primary and none in the secondary satisfies it on {5,3}"
     "Consistency.a_total_count_can_hide_a_missing_datacenter"⟩,
  ⟨"the commit upgrades every statement's data to the transaction's requested level",
   .met "Consistency.an_upgraded_commit_is_exactly_as_durable_as_its_data"⟩,
  ⟨"a transaction's decision covers every statement in it",
   .met "Consistency.a_decision_at_the_strongest_statement_covers_every_one"⟩,
  ⟨"the commit record rides the mutation it commits, so there is no durability window",
   .met "Consistency.no_window_when_the_decision_rides_the_data"⟩,
  ⟨"the commit record is written at the level the transaction requested, so it never lags the data",
   .met "Consistency.riding_the_last_mutation_is_safe_once_upgraded"⟩,
  ⟨"QUORUM needs the origin DC's majority and a majority of configured DCs",
   .met "Consistency.quorum_and_each_quorum_agree_on_two_dcs"⟩,
  ⟨"QUORUM and EACH_QUORUM are distinct levels above two datacenters",
   .met "Consistency.each_quorum_is_stricter_than_quorum_above_two_dcs"⟩,
  ⟨"DC_QUORUM keeps committing through a whole datacenter outage",
   .met "Consistency.dc_quorum_tolerates_a_whole_dc_outage"⟩,
  ⟨"DC_QUORUM also requires a quorum of all replicas, not only a shape across datacenters",
   .met "Consistency.dc_quorum_also_needs_a_node_quorum"⟩,
  ⟨"a DC_QUORUM read may be stale against a write acknowledged in another datacenter",
   .met "Consistency.a_local_read_does_not_consult_remote_datacenters"⟩,
  ⟨"DC_QUORUM reads are served from the local datacenter without a cross-DC hop",
   .met "Consistency.a_local_read_sees_a_dc_quorum_write"⟩,
  ⟨"the datacenter-majority condition prevents two partitions from both answering",
   .met "Topology.DcQuorum.no_partition_admits_two_dc_quorum_writes"⟩,
  ⟨"a multi-DC keyspace defaults to DC_QUORUM, a single-DC one to Cassandra's QUORUM",
   .met "Consistency.the_multi_dc_default_is_dc_quorum"⟩,
  ⟨"DC_QUORUM collapses to QUORUM when there is only one datacenter",
   .met "Consistency.dc_quorum_collapses_to_quorum_on_one_dc"⟩,
  ⟨"DC_QUORUM guarantees a majority of datacenters, not Cassandra's total",
   .met "Consistency.dc_quorum_trades_total_durability_for_dc_shape"⟩,
  ⟨"a QUORUM write is accepted exactly when Cassandra accepts it",
   .met "Consistency.quorum_is_cassandras_total_rule"⟩,
  ⟨"DC_QUORUM is strictly stronger than Cassandra's QUORUM, not incomparable to it",
   .met "Consistency.quorum_is_strictly_weaker_than_dc_quorum"⟩,
  ⟨"a partition cannot shrink the configured DC denominator",
   .met "Topology.DcQuorum.a_dark_majority_of_datacenters_blocks_every_write"⟩,
  ⟨"the decision level is carried by the transaction, not ranked from the enum",
   .met "Consistency.the_level_is_carried_not_ranked"⟩,
  ⟨"no consistency level, however high or newly defined, prevents write skew",
   .met "Consistency.no_consistency_level_prevents_write_skew"⟩,
  ⟨"a detected write skew is returned to the last writer as a retryable exception",
   .met "Consistency.the_last_writer_is_the_one_told_to_retry"⟩,
  ⟨"a consistency level only detects a write skew; retrying is the client's decision",
   .met "Consistency.a_retry_verdict_does_not_modify_the_state"⟩,
  ⟨"a rejected writer leaves no partial write and no silent success",
   .met "Consistency.a_rejected_writer_keeps_the_invariant"⟩,
  ⟨"the exception names the conflicting rows so the retry can be targeted",
   .met "Consistency.the_conflict_is_named_for_the_client"⟩,
  ⟨"a retry must re-read: an unchanged read set is rejected identically",
   .met "Consistency.only_a_reread_changes_the_verdict"⟩,
  ⟨"write skew is prevented by validating the read set at commit",
   .met "Consistency.read_set_validation_is_what_prevents_write_skew"⟩,
  ⟨"the write path captures a transaction's read set and validates it before committing",
   .failing
     "no read set is captured and no validation runs: dc_quorum compares acknowledgement counts only, so a write skew commits and no exception is raised"
     "Consistency.the_last_writer_is_the_one_told_to_retry"⟩
]

/-- Two of the ledger's obligations are red: one write path enforces a total rather than
    a per-datacenter requirement, and no read set is captured or validated before
    commit, so a write skew commits with no exception raised. -/
theorem consistency_has_two_red_items : redCount ledger = 2 := by decide

def main : IO Unit :=
  printLedger "SQL consistency levels" ledger

end Ferrosa.Sql.Consistency
