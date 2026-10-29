/-
  Ferrosa formal specification — Core.
  ===================================

  Self-contained (Lean core only, no Mathlib) model of the primitives every
  later trace leans on:

    * node sets over a finite universe `{0, …, n-1}`,
    * quorum arithmetic and the quorum-intersection theorem,
    * `ConsistencyLevel` and its ack thresholds, mirroring
      `ferrosa-cluster/src/consistency.rs::ConsistencyLevel::block_for`.

  The intersection theorem is the load-bearing one: every consistency claim in
  the Ferrosa specs (read-your-write, "QUORUM never returns stale", Accord fast
  path) reduces to it.  It is proved here once, for arbitrary `n`.
-/
namespace Ferrosa

/-! ## Node sets -/

abbrev Node := Nat

/-- A set of nodes drawn from the universe `{0, …, n-1}`. -/
abbrev SetU := Node → Bool

namespace SetU

/-- Cardinality relative to a universe of size `n`. -/
def card (n : Nat) (s : SetU) : Nat := ((List.range n).filter s).length

def inter (a b : SetU) : SetU := fun i => a i && b i

def union (a b : SetU) : SetU := fun i => a i || b i

def subset (n : Nat) (a b : SetU) : Prop := ∀ i, i < n → a i = true → b i = true

def ofList (l : List Node) : SetU := fun i => l.contains i

def univ : SetU := fun _ => true

def empty : SetU := fun _ => false

def singleton (i : Node) : SetU := fun j => j == i

@[simp] theorem card_univ (n : Nat) : (univ).card n = n := by
  have h : (List.range n).filter univ = List.range n :=
    List.filter_eq_self.mpr (by intro a _; rfl)
  simp [card, h, List.length_range]

@[simp] theorem card_empty (n : Nat) : (empty).card n = 0 := by
  have h : (List.range n).filter empty = [] := by
    rw [List.filter_eq_nil_iff]
    intro a _ ha
    exact Bool.noConfusion ha
  simp [card, h]

end SetU

/-! ## Quorum arithmetic -/

/-- `IsQuorum n q` — `q` is a strict majority of the universe of size `n`.
    Ferrosa's `block_for(QUORUM, rf) = rf / 2 + 1` is the same predicate one
    level down: an ack set is a quorum iff it holds at least `rf/2 + 1` nodes. -/
def IsQuorum (n : Nat) (q : SetU) : Prop := 2 * q.card n > n

/-! ### Counting lemma

The whole quorum theory rests on this: for any two predicates over a finite
list, the counts of `p` and `q` together overshoot the list length by at most
the count of `p ∧ q`.  Equivalently `|A| + |B| ≤ n + |A ∩ B|`. -/
theorem filter_and_length_le (l : List Node) (p q : Node → Bool) :
    (l.filter p).length + (l.filter q).length
      ≤ l.length + (l.filter (fun x => p x && q x)).length := by
  induction l with
  | nil => simp
  | cons a t ih =>
      simp only [List.filter_cons]
      cases hp : p a <;> cases hq : q a <;> simp [hp, hq] <;> omega

/-- Two strict majorities of the same universe always share a node.
    This is what makes `W + R > RF` give read-your-write. -/
theorem quorum_intersects {n : Nat} {a b : SetU}
    (ha : IsQuorum n a) (hb : IsQuorum n b) :
    ∃ i, i < n ∧ a i = true ∧ b i = true := by
  have hcount := filter_and_length_le (List.range n) a b
  simp only [IsQuorum, SetU.card, List.length_range] at ha hb hcount
  have hinter : 0 < ((List.range n).filter (fun x => a x && b x)).length := by omega
  obtain ⟨i, hi_mem⟩ := List.exists_mem_of_length_pos hinter
  simp only [List.mem_range, List.mem_filter, Bool.and_eq_true] at hi_mem
  exact ⟨i, hi_mem.1, hi_mem.2.1, hi_mem.2.2⟩

/-- Contrapositive form, used when reasoning about *unavailability* or
    divergence: two disjoint ack sets cannot both be quorums. -/
theorem no_two_disjoint_quorums {n : Nat} {a b : SetU}
    (hdisj : ∀ i, i < n → ¬(a i = true ∧ b i = true)) :
    ¬(IsQuorum n a ∧ IsQuorum n b) := by
  rintro ⟨ha, hb⟩
  obtain ⟨i, hi, hai, hbi⟩ := quorum_intersects ha hb
  exact hdisj i hi ⟨hai, hbi⟩

/-- The `W + R > RF` theorem: any write-ack set of size `w` and any read-ack
    set of size `r` share a node, hence a quorum read observes the write. -/
theorem quorum_wr_intersects {n : Nat} {w r : Nat}
    (hwr : r + w > n)
    {W R : SetU} (hW : W.card n = w) (hR : R.card n = r) :
    ∃ i, i < n ∧ W i = true ∧ R i = true := by
  have hcount := filter_and_length_le (List.range n) W R
  simp only [SetU.card, List.length_range] at hW hR hcount
  have hinter : 0 < ((List.range n).filter (fun x => W x && R x)).length := by omega
  obtain ⟨i, hi_mem⟩ := List.exists_mem_of_length_pos hinter
  simp only [List.mem_range, List.mem_filter, Bool.and_eq_true] at hi_mem
  exact ⟨i, hi_mem.1, hi_mem.2.1, hi_mem.2.2⟩

/-! ## Consistency levels

Mirrors `ConsistencyLevel` in `ferrosa-cluster/src/consistency.rs`, including
the two *different* threshold functions the code actually uses. -/

inductive ConsistencyLevel where
  | one | two | three | quorum | all
  | localOne | localQuorum | eachQuorum | serial | localSerial
  deriving DecidableEq, Repr

/-- `block_for`, verbatim: thresholds are computed from a single `rf`. -/
def blockFor : ConsistencyLevel → Nat → Nat
  | .one, _ => 1
  | .localOne, _ => 1
  | .two, rf => min 2 rf
  | .three, rf => min 3 rf
  | .quorum, rf => rf / 2 + 1
  | .localQuorum, rf => rf / 2 + 1
  | .serial, rf => rf / 2 + 1
  | .localSerial, rf => rf / 2 + 1
  | .all, rf => rf
  | .eachQuorum, _ => 0 -- `block_for` panics for EACH_QUORUM; modelled as 0.

/-- `block_for_dc`, verbatim. -/
def blockForDc : ConsistencyLevel → Nat → Nat
  | .eachQuorum, dcRf => dcRf / 2 + 1
  | .localQuorum, dcRf => dcRf / 2 + 1
  | .localSerial, dcRf => dcRf / 2 + 1
  | cl, dcRf => blockFor cl dcRf

/-- Every threshold but EACH_QUORUM's placeholder names at least one ack. -/
theorem blockFor_at_least_one (cl : ConsistencyLevel) (rf : Nat)
    (hrf : 0 < rf) (hne : cl ≠ .eachQuorum) : 1 ≤ blockFor cl rf := by
  cases cl <;> simp_all [blockFor] <;> omega

/-- Thresholds never exceed `rf` — the `block_for_never_exceeds_rf` proptest in
    `consistency.rs`, restated over all `rf`. -/
theorem blockFor_le_rf (cl : ConsistencyLevel) (rf : Nat)
    (hrf : 0 < rf) (hne : cl ≠ .eachQuorum) : blockFor cl rf ≤ rf := by
  have hdiv : rf / 2 + 1 ≤ rf := by
    have := Nat.div_add_mod rf 2
    have hmod : rf % 2 = 0 ∨ rf % 2 = 1 := by omega
    omega
  cases cl <;> simp_all [blockFor] <;> omega

/-- LOCAL_QUORUM computed against the **global** RF needs more acks than a
    single DC can ever supply.  With NetworkTopologyStrategy `{dc1:3, dc2:3}`
    the global RF is 6, so `block_for(LOCAL_QUORUM, 6) = 4`, while each DC holds
    only 3 replicas.  The generic coordinator paths (`write.rs:280`,
    `read.rs:561`) compute exactly this; the NTS paths (`write.rs:612`,
    `read.rs:1205`) compute `block_for_dc(local_rf) = 2`. -/
theorem localQuorum_global_rf_infeasible (dcRf totalRf : Nat)
    (h : blockFor .localQuorum totalRf > dcRf) :
    ∀ acks : SetU, acks.card dcRf ≤ dcRf →
      ¬(blockFor .localQuorum totalRf ≤ acks.card dcRf) := by
  intro acks hcard
  omega

/-- The same consistency level yields two different thresholds depending on
    which coordinator entry point serves the request. -/
theorem localQuorum_threshold_path_mismatch :
    blockFor .localQuorum (3 + 3) ≠ blockForDc .localQuorum 3 := by decide

/-! ## Replication factors as first-class values -/

/-- `RF = 2`: QUORUM needs every replica, so there is no fault tolerance. -/
theorem rf2_quorum_requires_all : blockFor .quorum 2 = 2 := by decide

/-- `RF = 3`: QUORUM needs two of three, tolerating exactly one replica loss. -/
theorem rf3_quorum_is_two : blockFor .quorum 3 = 2 := by decide

/-- `RF = 5`: QUORUM needs three, tolerating two replica losses. -/
theorem rf5_quorum_is_three : blockFor .quorum 5 = 3 := by decide

/-- At `RF = 3`, `QUORUM/QUORUM` satisfies `W + R > RF` (2 + 2 > 3). -/
theorem rf3_quorum_quorum_intersects :
    blockFor .quorum 3 + blockFor .quorum 3 > 3 := by decide

/-- At `RF = 2`, `ONE/ONE` does **not** satisfy `W + R > RF` (1 + 1 ≤ 2):
    a `ONE` read may miss a `ONE` write that acked on the other replica. -/
theorem rf2_one_one_no_intersection :
    ¬(blockFor .one 2 + blockFor .one 2 > 2) := by decide

end Ferrosa
