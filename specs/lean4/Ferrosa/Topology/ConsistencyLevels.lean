/-
  Ferrosa formal specification — a consistency level is a count, not a protocol.
  =============================================================================

  A consistency level decides how many replicas must acknowledge before the client is
  told the write succeeded, and that is the whole of it: `acks ≥ blockFor(level, rf)`,
  with no ballot, dependency or transaction order anywhere in the rule.  At `RF = 1`
  every level acknowledges on the first ack; `LOCAL_QUORUM` counts only the local
  datacenter's replicas and is computed from the local replication factor, never from the
  global sum, which is why the two entry points must not be confused.  A multi-statement
  transaction is acknowledged when every key it wrote has met its own level — still a
  conjunction of topology facts.  One correction is on record: A3 in `FINDINGS.md` claimed
  `LOCAL_QUORUM` is unsatisfiable on `{3,3}` because the global factor makes the
  threshold four, and that was wrong twice over — the NTS write path computes the local
  threshold correctly, and `blockFor` fed the global factor is the category error
  `Topology/TwoDc.lean` exhibits one size down.  Both arithmetic forms are transcribed
  from `ferrosa-cluster/src/consistency.rs` (`block_for`, `block_for_dc`).
-/
import Ferrosa.Core

namespace Ferrosa.Topology.ConsistencyLevels

open Ferrosa

/-! ## The ack rule, and why nothing about transactions appears in it -/

/-- A write is acknowledged when the acknowledgement count reaches the level's
    block-for value.  This is the whole of the consistency-level contract. -/
def ackOk (acks : Nat) (cl : ConsistencyLevel) (rf : Nat) : Bool :=
  acks ≥ blockFor cl rf

/-- The per-DC form, for levels whose block-for value is computed from a single
    DC's replication factor. -/
def ackOkLocal (dcAcks : Nat) (cl : ConsistencyLevel) (dcRf : Nat) : Bool :=
  dcAcks ≥ blockForDc cl dcRf

/-- The rule reads only `(acks, cl, rf)`.  Stated as a theorem so the separation
    from Accord is checkable rather than a comment: no dependency set, no ballot,
    no transaction order appears in the type. -/
theorem ack_rule_is_topological (acks : Nat) (cl : ConsistencyLevel) (rf : Nat) :
    ackOk acks cl rf = true ↔ acks ≥ blockFor cl rf := by
  simp [ackOk]

/-- **`RF = 1` acknowledges on the first ack, at every level.**  With a single
    replica there is nothing to wait for; `QUORUM`, `ALL`, `LOCAL_QUORUM` and
    `ONE` all reduce to the same thing.  (`EACH_QUORUM` is modelled as 0 because
    `block_for` panics for it and `block_for_dc` serves it.) -/
theorem one_ack_suffices_at_rf_one (cl : ConsistencyLevel) : ackOk 1 cl 1 = true := by
  cases cl <;> decide

/-- `RF = 3` at `QUORUM` needs two acks — the counting argument that gives
    `W + R > RF` with a matching read. -/
theorem quorum_rf3_needs_two : blockFor .quorum 3 = 2 := by decide

/-- `LOCAL_QUORUM` in a three-replica DC needs two *local* acks. -/
theorem local_quorum_rf3_needs_two : blockForDc .localQuorum 3 = 2 := by decide

/-! ## A transaction's ack is a conjunction of per-key acks -/

/-- A multi-statement transaction is acknowledged when every key it wrote has
    met its own consistency level.  Still topology: the rule composes over the
    keys, and the keys are independent. -/
def txnAckOk (keys : List Key) (acksOf : Key → Nat) (cl : ConsistencyLevel)
    (rf : Nat) : Bool :=
  keys.all (fun k => ackOk (acksOf k) cl rf)

/-- A transaction is acknowledged exactly when every key it wrote has met its own
    level, spelled out as a `∀` over the keys — which is all the composition there
    is. -/
theorem txnAckOk_iff (keys : List Key) (acksOf : Key → Nat) (cl : ConsistencyLevel)
    (rf : Nat) : txnAckOk keys acksOf cl rf = true ↔ ∀ k ∈ keys, ackOk (acksOf k) cl rf = true := by
  simp [txnAckOk, List.all_eq_true]

/-- A transaction that wrote nothing is trivially acknowledged; one that wrote a
    single key reduces to that key's rule.  Neither depends on ordering. -/
theorem txnAckOk_nil (acksOf : Key → Nat) (cl : ConsistencyLevel) (rf : Nat) :
    txnAckOk [] acksOf cl rf = true := by simp [txnAckOk]

/-- A one-key transaction's verdict is that key's ack rule and nothing more, so a
    single-key transaction never waits on a second key's replicas. -/
theorem txnAckOk_single (k : Key) (acksOf : Key → Nat) (cl : ConsistencyLevel) (rf : Nat) :
    txnAckOk [k] acksOf cl rf = ackOk (acksOf k) cl rf := by simp [txnAckOk]

/-! ## `LOCAL_QUORUM` is a function of the local RF only -/

/-- The local quorum of a DC holding `localRf` replicas.  Note the absence of any
    global replication factor from the signature: on an NTS keyspace the global
    RF is not an input to a LOCAL level. -/
def localQuorumThreshold (localRf : Nat) : Nat := blockForDc .localQuorum localRf

/-- Stated as an equation: the threshold is `localRf / 2 + 1`, whatever any other
    DC's replica count happens to be. -/
theorem localQuorumThreshold_eq (localRf : Nat) : localQuorumThreshold localRf = localRf / 2 + 1 := rfl

/-- Feeding `blockFor` the *global* RF instead gives a different answer, which is
    why the two entry points must not be confused. -/
theorem global_rf_would_over_ask_2_2 : blockFor .localQuorum 4 > localQuorumThreshold 2 := by
  decide

/-- On `{3,3}` the global factor is six, so a `LOCAL_QUORUM` computed from it asks
    four acks where the local datacenter's three replicas need only two.  This is the
    same category error as `{2,2}`, one size up, and it is why the two thresholds are
    never interchangeable. -/
theorem global_rf_would_over_ask_3_3 : blockFor .localQuorum 6 > localQuorumThreshold 3 := by
  decide

/-! ## QUORUM on two uniform datacenters, and the parity that decides whether the numbers agree -/

/-- `{3, 3}`: the global-RF threshold (4) coincides with `quorum_local_1 +
    quorum_local_2` (2 + 2).  This is why the numbers looked right. -/
theorem quorum_2x3_matches_per_dc_sum :
    blockFor .quorum 6 = blockForDc .quorum 3 + blockForDc .quorum 3 := by decide

/-- `{2, 2}`: the global-RF threshold (3) is *less* than `2 + 2`, so a transaction
    can be acknowledged without a quorum in the second DC.  This is the direction
    that is actually unsafe, and it bites exactly at the two-DC `{2,2}` rung of
    the replication ladder. -/
theorem quorum_2x2_undershoots_per_dc_sum :
    blockFor .quorum 4 < blockForDc .quorum 2 + blockForDc .quorum 2 := by decide

/-- The general shape: for two uniform DCs of size `r`, the global threshold is
    `r + 1` while the per-DC sum is `2 * (r / 2 + 1)`, which agree iff `r` is odd.
    Recorded for the sizes in play. -/
theorem quorum_threshold_parity :
    (blockFor .quorum 6 = blockForDc .quorum 3 + blockForDc .quorum 3) ∧
      (blockFor .quorum 4 < blockForDc .quorum 2 + blockForDc .quorum 2) ∧
      (blockFor .quorum 8 < blockForDc .quorum 4 + blockForDc .quorum 4) := by
  decide

/-! ## A threshold is a count, not a set condition -/

/-- For `{3, 3}`, four acknowledgements satisfy QUORUM while dc2 has contributed
    only one — fewer than its own quorum of two.  Nothing in the counting rule
    rejects that split, which is what "2 local + 2 remote" requires and what
    `total_acks >= required` does not express. -/
theorem quorum_threshold_allows_uneven_split :
    1 < blockForDc .quorum 3 ∧ (3 + 1) ≥ blockFor .quorum (3 + 3) := by decide

/-- And on `{2, 2}`: three acks satisfy QUORUM, and the only way to reach three is
    both of dc1's replicas plus one of dc2's — a quorum in dc1 and a non-quorum in
    dc2.  Still "2 local + 1 remote", not "2 + 2". -/
theorem quorum_threshold_allows_2_1_split :
    (2 + 1) ≥ blockFor .quorum (2 + 2) ∧ 1 < blockForDc .quorum 2 := by decide

end Ferrosa.Topology.ConsistencyLevels
