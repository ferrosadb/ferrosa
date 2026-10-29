/-
  Ferrosa formal specification — consistency levels as a topology guarantee.
  ==========================================================================

  **This is not an Accord file.**  A consistency level is an acknowledgement
  rule over a replica set:

      a write at CL `x` with replication factor `rf` is acknowledged
      once `acks ≥ blockFor(x, rf)` replicas have acknowledged it.

  Nothing in that sentence mentions transactions, dependencies, ballots or
  ordering.  `RF=1` acknowledges on the first ack at every level; `LOCAL_QUORUM`
  acknowledges once the *local* DC's quorum has acked; `QUORUM` on an NTS
  keyspace acknowledges once a quorum exists in every DC.  A plain fan-out write
  with no consensus protocol at all obeys the same rule.

  Accord contributes two things on top, and both are about *ordering*:

    * the **commit decision** — the transaction is committed once the
      fast/slow quorum of the electorate has accepted it (§4.2, §4.3);
    * the **dep-wait before the read phase** — the transaction must not observe
      a state that is missing a conflicting predecessor (§4.5).

  Those are modelled in `Ferrosa.Accord.Protocol` and `Ferrosa.Accord.Recovery`,
  and they compose with — they do not replace — the ack rule here.  A
  multi-statement transaction is acknowledged when *every* key it wrote has met
  its own CL, which is again a conjunction of topology facts (`txnAckOk` below).

  The retraction this file also records: an earlier finding in `FINDINGS.md`
  (A3) claimed `LOCAL_QUORUM` is unsatisfiable on `{3,3}` because the global RF
  makes the threshold 4.  That was wrong twice over — `write_path.rs:938` routes
  NTS keyspaces to `coordinate_write_nts`, which computes the local threshold
  correctly, and a local level never consults the global RF at all.

  Code references (`ferrosa-cluster/src`):

    * `consistency.rs:31` `block_for`, `:46` `block_for_dc`.
    * `write_path.rs:938` dispatches `Simple` → `coordinate_write_with`,
      `NetworkTopology` → `coordinate_write_nts`.
    * `coordinator/write.rs:612` computes the NTS required count
      (`LocalQuorum => block_for_dc(local_rf)`), `:630` the rest
      (`_ => block_for(strategy.replication_factor())`).
    * `ring/strategy.rs:23` — `replication_factor()` for NTS is the **sum** of
      the per-DC factors.
    * `coordinator/write.rs:753` counts `total_acks >= required` for every level
      except `EachQuorum`, whose per-DC counters are the only ones consulted.
-/
import Ferrosa.Core

namespace Ferrosa.Topology.ConsistencyLevels

open Ferrosa

/-! ## The ack rule -/

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

theorem txnAckOk_iff (keys : List Key) (acksOf : Key → Nat) (cl : ConsistencyLevel)
    (rf : Nat) : txnAckOk keys acksOf cl rf = true ↔ ∀ k ∈ keys, ackOk (acksOf k) cl rf = true := by
  simp [txnAckOk, List.all_eq_true]

/-- A transaction that wrote nothing is trivially acknowledged; one that wrote a
    single key reduces to that key's rule.  Neither depends on ordering. -/
theorem txnAckOk_nil (acksOf : Key → Nat) (cl : ConsistencyLevel) (rf : Nat) :
    txnAckOk [] acksOf cl rf = true := by simp [txnAckOk]

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

theorem global_rf_would_over_ask_3_3 : blockFor .localQuorum 6 > localQuorumThreshold 3 := by
  decide

/-! ## `QUORUM` on two uniform DCs -/

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
