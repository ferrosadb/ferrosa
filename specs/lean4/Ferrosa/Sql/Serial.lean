/-
  Ferrosa formal specification — `serial` / `bigserial` ids (decision D15).
  ========================================================================

  PostgreSQL's `bigserial` reads a sequence. Ferrosa has no sequence that every node
  could read without coordinating, so a `bigserial` column instead gets a
  server-generated 64-bit id built from three fields:

      id = millis · 2^22 + node · 2^12 + seq

  * `millis` — milliseconds since the Ferrosa id epoch, 41 bits;
  * `node`   — the minting node's id tag, 10 bits;
  * `seq`    — a per-node counter within one millisecond, 12 bits.

  The id is positive and fits in a signed 64-bit `bigint`. It is not dense and not
  ordered across nodes. Two ids minted on nodes with **distinct** node tags never collide,
  whatever their clocks say; two ids minted on one node never collide because the node's
  generator only ever moves `(millis, seq)` forward. Distinct node tags are a precondition
  the deployment must establish; this module proves what follows from it.

  `serial` (32-bit) and `smallserial` (16-bit) cannot hold such an id, so they are refused
  (`serial_cannot_hold_an_id`).

  Source: `ferrosa-postgres/src/serial_id.rs` (`compose`, `SerialIdGenerator::next`).
-/

namespace Ferrosa.Sql.Serial

/-- Bits per field. -/
def millisBits : Nat := 41
def nodeBits : Nat := 10
def seqBits : Nat := 12

/-- The id of `(millis, node, seq)`. -/
def compose (millis node seq : Nat) : Nat :=
  millis * 4194304 + node * 4096 + seq

/-- A field triple is well formed when each field fits its width. -/
def WellFormed (millis node seq : Nat) : Prop :=
  millis < 2199023255552 ∧ node < 1024 ∧ seq < 4096

/-! ## The fields can be read back

Each field occupies its own bit range, so the id determines the triple. That is the whole uniqueness
argument: an id collision is a triple collision.

-/

theorem seq_of_compose (m n s : Nat) (_hn : n < 1024) (hs : s < 4096) :
    compose m n s % 4096 = s := by
  unfold compose; omega

theorem node_of_compose (m n s : Nat) (hn : n < 1024) (hs : s < 4096) :
    compose m n s / 4096 % 1024 = n := by
  unfold compose; omega

theorem millis_of_compose (m n s : Nat) (hn : n < 1024) (hs : s < 4096) :
    compose m n s / 4194304 = m := by
  unfold compose; omega

/-- **Composition is injective on well-formed triples.** -/
theorem compose_injective (m n s m' n' s' : Nat) (h : WellFormed m n s) (h' : WellFormed m' n' s')
    (heq : compose m n s = compose m' n' s') : m = m' ∧ n = n' ∧ s = s' := by
  obtain ⟨_, hn, hs⟩ := h
  obtain ⟨_, hn', hs'⟩ := h'
  refine ⟨?_, ?_, ?_⟩
  · rw [← millis_of_compose m n s hn hs, ← millis_of_compose m' n' s' hn' hs', heq]
  · rw [← node_of_compose m n s hn hs, ← node_of_compose m' n' s' hn' hs', heq]
  · rw [← seq_of_compose m n s hn hs, ← seq_of_compose m' n' s' hn' hs', heq]

/-- **Unique across nodes, without coordination.**  Ids minted on nodes with distinct tags differ,
    whatever the two clocks and counters say. -/
theorem distinct_nodes_never_collide (m n s m' n' s' : Nat) (h : WellFormed m n s)
    (h' : WellFormed m' n' s') (hne : n ≠ n') : compose m n s ≠ compose m' n' s' :=
  fun heq => hne (compose_injective m n s m' n' s' h h' heq).2.1

/-- **Positive and within `bigint`.**  A well-formed id with a non-zero clock is positive and below
    `2^63`, so it is a positive signed 64-bit value. -/
theorem fits_a_positive_bigint (m n s : Nat) (h : WellFormed m n s) (hm : 0 < m) :
    0 < compose m n s ∧ compose m n s < 9223372036854775808 := by
  obtain ⟨hm', hn, hs⟩ := h
  unfold compose
  omega

/-- **`serial` cannot hold an id.**  Any id minted after the first millisecond already exceeds
    `2^31 - 1`, the largest `integer`, once the clock passes 512 ms — which it has, for every id
    minted after the epoch. So `serial` and `smallserial` columns are refused rather than
    truncated. -/
theorem serial_cannot_hold_an_id (m n s : Nat) (hm : 512 ≤ m) :
    2147483647 < compose m n s := by
  unfold compose
  omega

/-! ## One node's generator

A node's generator keeps the last `(millis, seq)` it issued. A new id uses the wall clock when it has
advanced; otherwise it keeps the last millisecond and bumps `seq`; when `seq` is exhausted it borrows the
next millisecond. Either way the pair strictly increases, so one node never issues an id twice.

-/

/-- The generator's state: the last pair issued. -/
structure Gen where
  millis : Nat
  seq : Nat
  deriving DecidableEq, Repr

/-- The next pair, given the wall clock `now`. -/
def Gen.next (g : Gen) (now : Nat) : Gen :=
  if g.millis < now then ⟨now, 0⟩
  else if g.seq + 1 < 4096 then ⟨g.millis, g.seq + 1⟩
  else ⟨g.millis + 1, 0⟩

/-- Pairs ordered lexicographically. -/
def Gen.lt (a b : Gen) : Prop := a.millis < b.millis ∨ (a.millis = b.millis ∧ a.seq < b.seq)

/-- **One node never repeats itself.**  Every step strictly increases the issued pair. -/
theorem next_strictly_increases (g : Gen) (now : Nat) : Gen.lt g (g.next now) := by
  unfold Gen.next Gen.lt
  by_cases h1 : g.millis < now
  · simp [h1]
  · by_cases h2 : g.seq + 1 < 4096
    · simp [h1, h2]
    · simp [h1, h2]

/-- The sequence field stays in range, so a generated triple is well formed whenever its clock is. -/
theorem next_seq_in_range (g : Gen) (now : Nat) (_hs : g.seq < 4096) : (g.next now).seq < 4096 := by
  unfold Gen.next
  by_cases h1 : g.millis < now
  · simp [h1]
  · by_cases h2 : g.seq + 1 < 4096
    · simp [h1, h2]
    · simp [h1, h2]

/-- Two distinct well-formed pairs on one node give distinct ids. -/
theorem same_node_distinct_pairs (n : Nat) (a b : Gen) (hn : n < 1024)
    (ha : a.millis < 2199023255552 ∧ a.seq < 4096) (hb : b.millis < 2199023255552 ∧ b.seq < 4096)
    (hlt : Gen.lt a b) : compose a.millis n a.seq ≠ compose b.millis n b.seq := by
  intro heq
  obtain ⟨hm, _, hs⟩ := compose_injective a.millis n a.seq b.millis n b.seq
    ⟨ha.1, hn, ha.2⟩ ⟨hb.1, hn, hb.2⟩ heq
  unfold Gen.lt at hlt
  omega

end Ferrosa.Sql.Serial
