/-
  Ferrosa formal specification — DC_QUORUM, for every topology.
  ==============================================================

  Source: `ferrosa-cluster/src/coordinator/dc_quorum.rs` (PR #634), `progress` for
  `ConsistencyLevel::DcQuorum` and `local_progress`.  The code answers a DC_QUORUM write with three
  gates, evaluated over the *configured* replication factors and never over the reachable ones:

    1. the origin datacenter reaches its own majority      (`origin_rf / 2 + 1 ≤ count_in_dc(origin)`)
    2. a strict majority of configured datacenters each reach theirs
                                                            (`configured_dcs / 2 + 1 ≤ #{dc : count > rf/2}`)
    3. a quorum of all configured replicas                  (`rf_total / 2 + 1 ≤ total`)

  and a DC_QUORUM read with gate 1 alone.  Datacenters configured with `rf = 0` are filtered out of
  every count (`.filter(|rf| **rf > 0)`), and an origin with `rf = 0` fails closed.

  `Sql/Consistency.lean` checks these rules on concrete topologies.  This module proves the claims
  the code's comments rest on for **every** topology, every pair of origins, and every ack set:

    * `dc_gate_forces_a_shared_replica` — two writes that pass gate 2 share a replica.  This is the
      split-brain argument: `no_partition_admits_two_dc_quorum_writes`.
    * `node_gate_forces_a_shared_replica` — gate 3 alone does the same, so a DC_QUORUM write and a
      plain Cassandra QUORUM write on the same keyspace also intersect.
    * `a_local_read_sees_every_write_that_reached_its_datacenter` and its same-origin corollary,
      against `a_read_in_another_datacenter_can_miss_a_dc_quorum_write`.
    * `write_is_monotone` — a late or duplicated acknowledgement can never revoke a verdict.
    * `a_dark_majority_of_datacenters_blocks_every_write` — the general form of "a partition cannot
      shrink the denominator".
    * `a_zero_rf_datacenter_is_inert` — appending an `rf = 0` datacenter changes no verdict.

  Replicas of datacenter `d` are the universe `{0, …, rf d - 1}`, and an acknowledgement set assigns
  each datacenter a `SetU` over its own universe.  The code's `count_in_dc(dc)` is that set's
  cardinality, and its `total` is the sum of them (distinct eligible responders).
-/
import Ferrosa.Core

namespace Ferrosa.Topology.DcQuorum

open Ferrosa

/-! ## The rule, transcribed from `dc_quorum.rs` -/

/-- Configured replicas per datacenter, indexed by datacenter (`dc_replication_factors`). -/
abbrev Rfs := List Nat

/-- A datacenter's configured replication factor; an unconfigured index has none. -/
def rfOf (rfs : Rfs) (d : Nat) : Nat := rfs.getD d 0

/-- The code filters `rf > 0` out of every count. -/
def eligible (rfs : Rfs) (d : Nat) : Bool := decide (0 < rfOf rfs d)

/-- `count_in_dc(dc) > rf / 2`. -/
def dcMajority (rf c : Nat) : Bool := decide (rf / 2 < c)

/-- The datacenters that count: configured with at least one replica. -/
def eligibleDcs (rfs : Rfs) : List Nat := (List.range rfs.length).filter (eligible rfs)

/-- `configured.values().filter(|rf| **rf > 0).count()`. -/
def configuredDcs (rfs : Rfs) : Nat := (eligibleDcs rfs).length

/-- The eligible datacenters that reached their own majority. -/
def votes (rfs : Rfs) (cnt : Nat → Nat) : List Nat :=
  (eligibleDcs rfs).filter fun d => dcMajority (rfOf rfs d) (cnt d)

/-- `rf_total`: the configured replicas.  An `rf = 0` datacenter contributes nothing to a sum, so
    the code's filter is unnecessary here. -/
def rfTotal (rfs : Rfs) : Nat := ((List.range rfs.length).map (rfOf rfs)).sum

/-- Gate 1: the origin's majority.  An origin configured with `rf = 0` fails closed. -/
def originGate (rfs : Rfs) (origin : Nat) (cnt : Nat → Nat) : Bool :=
  eligible rfs origin && dcMajority (rfOf rfs origin) (cnt origin)

/-- Gate 2: a strict majority of configured datacenters, each at its own majority. -/
def dcGate (rfs : Rfs) (cnt : Nat → Nat) : Bool :=
  decide (configuredDcs rfs / 2 + 1 ≤ (votes rfs cnt).length)

/-- Gate 3: a quorum of all configured replicas. -/
def nodeGate (rfs : Rfs) (total : Nat) : Bool := decide (rfTotal rfs / 2 + 1 ≤ total)

/-- A DC_QUORUM write: `progress(DcQuorum, …).satisfied()`. -/
def write (rfs : Rfs) (origin : Nat) (cnt : Nat → Nat) (total : Nat) : Bool :=
  originGate rfs origin cnt && dcGate rfs cnt && nodeGate rfs total

/-- A DC_QUORUM read: `local_progress(…).satisfied()`, the origin's majority alone. -/
def read (rfs : Rfs) (origin : Nat) (cnt : Nat → Nat) : Bool := originGate rfs origin cnt

/-! ## Acknowledgement sets -/

/-- The replicas of each datacenter that acknowledged, as a set over that datacenter's universe. -/
abbrev Acks := Nat → SetU

/-- `count_in_dc(d)`: distinct acknowledging replicas of `d`. -/
def count (rfs : Rfs) (A : Acks) (d : Nat) : Nat := (A d).card (rfOf rfs d)

/-- `total`: distinct acknowledging replicas across the configured datacenters. -/
def total (rfs : Rfs) (A : Acks) : Nat :=
  ((List.range rfs.length).map (count rfs A)).sum

/-- A write whose verdict is computed from an actual acknowledgement set. -/
def writeOf (rfs : Rfs) (origin : Nat) (A : Acks) : Bool :=
  write rfs origin (count rfs A) (total rfs A)

/-- A read whose verdict is computed from an actual acknowledgement set. -/
def readOf (rfs : Rfs) (origin : Nat) (A : Acks) : Bool := read rfs origin (count rfs A)

/-- Two acknowledgement sets share a replica somewhere in the configured topology. -/
def SharesReplica (rfs : Rfs) (A B : Acks) : Prop :=
  ∃ d i, d < rfs.length ∧ i < rfOf rfs d ∧ A d i = true ∧ B d i = true

/-! ## The code's majority test is `IsQuorum` -/

/-- `count > rf / 2` is exactly a strict majority of the datacenter's universe. -/
theorem dcMajority_count_iff (rf : Nat) (s : SetU) :
    dcMajority rf (s.card rf) = true ↔ IsQuorum rf s := by
  simp only [dcMajority, IsQuorum, decide_eq_true_eq]
  omega

/-- A datacenter's majority is never zero acknowledgements. -/
theorem dcMajority_pos {rf c : Nat} (h : dcMajority rf c = true) : 0 < c := by
  simp only [dcMajority, decide_eq_true_eq] at h
  omega

/-! ## Gate 2: two passing writes share a datacenter, then a replica -/

/-- **Two writes that pass the datacenter gate share a datacenter where both hold a majority.**
    Two strict majorities of the eligible datacenters overlap — the counting lemma one level up. -/
theorem dc_gate_forces_a_shared_datacenter {rfs : Rfs} {cA cB : Nat → Nat}
    (hA : dcGate rfs cA = true) (hB : dcGate rfs cB = true) :
    ∃ d, d < rfs.length ∧ eligible rfs d = true ∧
      dcMajority (rfOf rfs d) (cA d) = true ∧ dcMajority (rfOf rfs d) (cB d) = true := by
  let p : Nat → Bool := fun d => dcMajority (rfOf rfs d) (cA d)
  let q : Nat → Bool := fun d => dcMajority (rfOf rfs d) (cB d)
  have hA' : (eligibleDcs rfs).length / 2 + 1 ≤ ((eligibleDcs rfs).filter p).length :=
    of_decide_eq_true hA
  have hB' : (eligibleDcs rfs).length / 2 + 1 ≤ ((eligibleDcs rfs).filter q).length :=
    of_decide_eq_true hB
  have hcount : ((eligibleDcs rfs).filter p).length + ((eligibleDcs rfs).filter q).length
      ≤ (eligibleDcs rfs).length + ((eligibleDcs rfs).filter (fun d => p d && q d)).length :=
    filter_and_length_le (eligibleDcs rfs) p q
  have hpos : 0 < ((eligibleDcs rfs).filter (fun d => p d && q d)).length := by omega
  obtain ⟨d, hd⟩ := List.exists_mem_of_length_pos hpos
  simp only [List.mem_filter, Bool.and_eq_true, eligibleDcs, List.mem_range] at hd
  exact ⟨d, hd.1.1, hd.1.2, hd.2.1, hd.2.2⟩

/-- **Gate 2 alone forces a shared replica**, for every topology and every pair of origins. -/
theorem dc_gate_forces_a_shared_replica {rfs : Rfs} {A B : Acks}
    (hA : dcGate rfs (count rfs A) = true) (hB : dcGate rfs (count rfs B) = true) :
    SharesReplica rfs A B := by
  obtain ⟨d, hd, _, hmA, hmB⟩ := dc_gate_forces_a_shared_datacenter hA hB
  have qA := (dcMajority_count_iff _ _).mp hmA
  have qB := (dcMajority_count_iff _ _).mp hmB
  obtain ⟨i, hi, hAi, hBi⟩ := quorum_intersects qA qB
  exact ⟨d, i, hd, hi, hAi, hBi⟩

/-- **Any two DC_QUORUM writes share a replica**, whatever their origins. -/
theorem dc_quorum_writes_share_a_replica {rfs : Rfs} {oA oB : Nat} {A B : Acks}
    (hA : writeOf rfs oA A = true) (hB : writeOf rfs oB B = true) :
    SharesReplica rfs A B := by
  simp only [writeOf, write, Bool.and_eq_true] at hA hB
  exact dc_gate_forces_a_shared_replica hA.1.2 hB.1.2

/-- **No partition admits two DC_QUORUM writes.**  If two sides of a partition hold disjoint
    replicas, at most one of them can acknowledge — for every topology, not only `{3,3,3}`. -/
theorem no_partition_admits_two_dc_quorum_writes {rfs : Rfs} {oA oB : Nat} {A B : Acks}
    (hdisj : ∀ d i, d < rfs.length → i < rfOf rfs d → ¬(A d i = true ∧ B d i = true)) :
    ¬(writeOf rfs oA A = true ∧ writeOf rfs oB B = true) := by
  rintro ⟨hA, hB⟩
  obtain ⟨d, i, hd, hi, hAi, hBi⟩ := dc_quorum_writes_share_a_replica hA hB
  exact hdisj d i hd hi ⟨hAi, hBi⟩

/-! ## Gate 3: a node quorum intersects any node quorum -/

/-- Replicas of `d` acknowledged by both sets. -/
def common (rfs : Rfs) (A B : Acks) (d : Nat) : Nat :=
  ((List.range (rfOf rfs d)).filter (fun i => A d i && B d i)).length

/-- The counting lemma, summed over any list of datacenters. -/
theorem sum_count_le (rfs : Rfs) (A B : Acks) (ds : List Nat) :
    (ds.map (count rfs A)).sum + (ds.map (count rfs B)).sum
      ≤ (ds.map (rfOf rfs)).sum + (ds.map (common rfs A B)).sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have h := filter_and_length_le (List.range (rfOf rfs d)) (A d) (B d)
      simp only [List.length_range] at h
      simp only [List.map_cons, List.sum_cons, count, SetU.card, common] at ih ⊢
      omega

/-- A positive sum has a positive term. -/
theorem exists_pos_of_sum_pos (ds : List Nat) (f : Nat → Nat) (h : 0 < (ds.map f).sum) :
    ∃ d, d ∈ ds ∧ 0 < f d := by
  induction ds with
  | nil => simp at h
  | cons d ds ih =>
      simp only [List.map_cons, List.sum_cons] at h
      by_cases hd : 0 < f d
      · exact ⟨d, List.mem_cons_self, hd⟩
      · obtain ⟨e, he, hfe⟩ := ih (by omega)
        exact ⟨e, List.mem_cons_of_mem d he, hfe⟩

/-- **Gate 3 alone forces a shared replica.**  Two acknowledgement sets that each hold a quorum of
    all configured replicas overlap in some datacenter, wherever their acknowledgements landed. -/
theorem node_gate_forces_a_shared_replica {rfs : Rfs} {A B : Acks}
    (hA : nodeGate rfs (total rfs A) = true) (hB : nodeGate rfs (total rfs B) = true) :
    SharesReplica rfs A B := by
  have hA' : rfTotal rfs / 2 + 1 ≤ total rfs A := of_decide_eq_true hA
  have hB' : rfTotal rfs / 2 + 1 ≤ total rfs B := of_decide_eq_true hB
  simp only [total, rfTotal] at hA' hB'
  have hsum := sum_count_le rfs A B (List.range rfs.length)
  obtain ⟨d, hd, hpos⟩ := exists_pos_of_sum_pos (List.range rfs.length) (common rfs A B)
    (by omega)
  obtain ⟨i, hi⟩ := List.exists_mem_of_length_pos hpos
  simp only [List.mem_filter, List.mem_range, Bool.and_eq_true] at hi hd
  exact ⟨d, i, hd, hi.1, hi.2.1, hi.2.2⟩

/-- Cassandra's QUORUM on the same keyspace: a quorum of all replicas, anywhere. -/
def cassandraQuorumOf (rfs : Rfs) (A : Acks) : Bool := nodeGate rfs (total rfs A)

/-- **A DC_QUORUM write is a Cassandra QUORUM write**: gate 3 is Cassandra's rule. -/
theorem dc_quorum_write_is_a_cassandra_quorum_write {rfs : Rfs} {o : Nat} {A : Acks}
    (h : writeOf rfs o A = true) : cassandraQuorumOf rfs A = true := by
  simp only [writeOf, write, Bool.and_eq_true] at h
  exact h.2

/-- **Mixed levels still intersect.**  A client writing at DC_QUORUM and another writing at
    Cassandra's QUORUM on the same keyspace cannot both succeed on disjoint replicas. -/
theorem dc_quorum_and_cassandra_quorum_writes_share_a_replica {rfs : Rfs} {o : Nat} {A B : Acks}
    (hA : writeOf rfs o A = true) (hB : cassandraQuorumOf rfs B = true) :
    SharesReplica rfs A B :=
  node_gate_forces_a_shared_replica (dc_quorum_write_is_a_cassandra_quorum_write hA) hB

/-! ## Reads: local, hop-free, and blind elsewhere -/

/-- **A local read sees every write that reached a majority of the reader's datacenter.** -/
theorem a_local_read_sees_every_write_that_reached_its_datacenter {rfs : Rfs} {r : Nat}
    {W R : Acks} (hR : readOf rfs r R = true)
    (hW : dcMajority (rfOf rfs r) (count rfs W r) = true) :
    ∃ i, i < rfOf rfs r ∧ W r i = true ∧ R r i = true := by
  simp only [readOf, read, originGate, Bool.and_eq_true] at hR
  exact quorum_intersects ((dcMajority_count_iff _ _).mp hW) ((dcMajority_count_iff _ _).mp hR.2)

/-- **A read in the write's origin datacenter always sees it**, for every topology. -/
theorem a_same_origin_read_sees_a_dc_quorum_write {rfs : Rfs} {o : Nat} {W R : Acks}
    (hW : writeOf rfs o W = true) (hR : readOf rfs o R = true) :
    ∃ i, i < rfOf rfs o ∧ W o i = true ∧ R o i = true := by
  simp only [writeOf, write, originGate, Bool.and_eq_true] at hW
  exact a_local_read_sees_every_write_that_reached_its_datacenter hR hW.1.1.2

/-- `{3,3,3}`: the write holds all of dc0 and two of dc1; the reader holds two of dc2. -/
def missRfs : Rfs := [3, 3, 3]
def missWrite : Acks := fun d i => (d == 0 && decide (i < 3)) || (d == 1 && decide (i < 2))
def missRead : Acks := fun d i => d == 2 && decide (i < 2)

/-- **A read in another datacenter can miss a DC_QUORUM write.**  The write passes all three gates
    from dc0, the read passes its gate in dc2, and they share no replica: the hop-free read is blind
    to replication it did not wait for.  This is the accepted cost the code comment names. -/
theorem a_read_in_another_datacenter_can_miss_a_dc_quorum_write :
    writeOf missRfs 0 missWrite = true ∧ readOf missRfs 2 missRead = true ∧
      ¬ SharesReplica missRfs missWrite missRead := by
  refine ⟨by decide, by decide, ?_⟩
  rintro ⟨d, i, _, _, hW, hR⟩
  simp only [missWrite, missRead, Bool.or_eq_true, Bool.and_eq_true, beq_iff_eq,
    decide_eq_true_eq] at hW hR
  omega

/-! ## Monotonicity: a verdict is never revoked -/

/-- A weaker filter keeps at least as many elements. -/
theorem filter_length_mono (l : List Nat) {p q : Nat → Bool} (h : ∀ a, p a = true → q a = true) :
    (l.filter p).length ≤ (l.filter q).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp only [List.filter_cons]
      cases hp : p a <;> cases hq : q a <;> simp
      · omega
      · exact Nat.le_succ_of_le ih
      · exact absurd (h a hp) (by simp [hq])
      · omega

theorem votes_length_mono {rfs : Rfs} {c c' : Nat → Nat} (h : ∀ d, c d ≤ c' d) :
    (votes rfs c).length ≤ (votes rfs c').length := by
  apply filter_length_mono
  intro d hd
  simp only [dcMajority, decide_eq_true_eq] at hd ⊢
  have := h d
  omega

/-- **More acknowledgements never revoke a write.**  The coordinator's counters only grow, so a
    late, slow or duplicated reply can turn an unmet verdict into a met one and never the reverse. -/
theorem write_is_monotone {rfs : Rfs} {o : Nat} {c c' : Nat → Nat} {t t' : Nat}
    (hc : ∀ d, c d ≤ c' d) (ht : t ≤ t') (h : write rfs o c t = true) : write rfs o c' t' = true := by
  simp only [write, originGate, dcGate, nodeGate, Bool.and_eq_true, decide_eq_true_eq,
    dcMajority] at h ⊢
  have hv := votes_length_mono (rfs := rfs) hc
  have ho := hc o
  refine ⟨⟨⟨h.1.1.1, by omega⟩, by omega⟩, by omega⟩

/-! ## The denominator is configuration -/

/-- The eligible datacenters that answered at all. -/
def reached (rfs : Rfs) (cnt : Nat → Nat) : List Nat := (eligibleDcs rfs).filter fun d => 0 < cnt d

/-- **A dark majority of datacenters blocks every write.**  If fewer than a strict majority of the
    configured datacenters answered at all, gate 2 fails, whatever the answering ones hold.  The
    requirement counts configured datacenters, so a partition cannot lower it. -/
theorem a_dark_majority_of_datacenters_blocks_every_write {rfs : Rfs} {o : Nat}
    {cnt : Nat → Nat} {t : Nat}
    (hdark : (reached rfs cnt).length < configuredDcs rfs / 2 + 1) :
    write rfs o cnt t = false := by
  have hsub : (votes rfs cnt).length ≤ (reached rfs cnt).length := by
    apply filter_length_mono
    intro d hd
    simpa using dcMajority_pos hd
  have hgate : dcGate rfs cnt = false := by
    simp only [dcGate, decide_eq_false_iff_not]
    omega
  simp [write, hgate]

/-! ## An `rf = 0` datacenter is inert -/

theorem rfOf_ge {rfs : Rfs} {d : Nat} (h : rfs.length ≤ d) : rfOf rfs d = 0 := by
  induction rfs generalizing d with
  | nil => simp [rfOf]
  | cons a t ih =>
      cases d with
      | zero => simp at h
      | succ d =>
          simp only [rfOf, List.getD_cons_succ] at ih ⊢
          exact ih (by simpa using h)

theorem rfOf_append_zero (rfs : Rfs) (d : Nat) : rfOf (rfs ++ [0]) d = rfOf rfs d := by
  induction rfs generalizing d with
  | nil =>
      cases d with
      | zero => simp [rfOf]
      | succ d => simp [rfOf]
  | cons a t ih =>
      cases d with
      | zero => simp [rfOf]
      | succ d =>
          simp only [rfOf, List.cons_append, List.getD_cons_succ] at ih ⊢
          exact ih d

theorem eligibleDcs_append_zero (rfs : Rfs) : eligibleDcs (rfs ++ [0]) = eligibleDcs rfs := by
  have he : eligible (rfs ++ [0]) = eligible rfs := by
    funext d; simp only [eligible, rfOf_append_zero]
  simp only [eligibleDcs, he, List.length_append, List.length_singleton, List.range_succ,
    List.filter_append]
  have : eligible rfs rfs.length = false := by simp [eligible, rfOf_ge (Nat.le_refl _)]
  simp [this]

theorem rfTotal_append_zero (rfs : Rfs) : rfTotal (rfs ++ [0]) = rfTotal rfs := by
  have hr : rfOf (rfs ++ [0]) = rfOf rfs := by funext d; exact rfOf_append_zero rfs d
  have h0 : rfOf rfs rfs.length = 0 := rfOf_ge (Nat.le_refl _)
  simp only [rfTotal, hr, List.length_append, List.length_singleton, List.range_succ,
    List.map_append, List.sum_append]
  simp [h0]

/-- **Configuring a datacenter with no replicas changes no verdict**, which is what the code's
    `rf > 0` filters are for: it neither raises the datacenter denominator nor the replica total. -/
theorem a_zero_rf_datacenter_is_inert (rfs : Rfs) (o : Nat) (cnt : Nat → Nat) (t : Nat) :
    write (rfs ++ [0]) o cnt t = write rfs o cnt t := by
  have hr : rfOf (rfs ++ [0]) = rfOf rfs := by funext d; exact rfOf_append_zero rfs d
  have he : eligible (rfs ++ [0]) = eligible rfs := by funext d; simp only [eligible, hr]
  unfold write originGate dcGate nodeGate votes configuredDcs
  rw [eligibleDcs_append_zero, rfTotal_append_zero, hr, he]

/-- And the origin gate fails closed on an `rf = 0` origin: no acknowledgement count passes it. -/
theorem a_zero_rf_origin_fails_closed (rfs : Rfs) (o : Nat) (cnt : Nat → Nat)
    (h : rfOf rfs o = 0) : read rfs o cnt = false := by
  simp [read, originGate, eligible, h]

end Ferrosa.Topology.DcQuorum
