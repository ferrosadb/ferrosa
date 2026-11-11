/-
  Deletes and expiry: what a tombstone hides, and the two ways a row comes back
  ===========================================================================

  A delete leaves a tombstone that beats every value at or below its timestamp, in either arrival
  order, so the row cannot return by being merged in a different sequence; a tie between a delete and
  an insert resolves to the delete, which is what stops a concurrent insert from undoing it. A TTL is
  materialised at write time into a stored local deletion time, so expiry compares against a number
  the coordinator wrote and a replica's clock never enters the answer — which is also why a write
  carrying a past `USING TIMESTAMP` is invisible the moment it arrives.

  Two ways a user can still lose. The stored local deletion time is an `i32`, so a TTL reaching past
  `2^31` seconds wraps negative and the cell reads as expired while the writer was told the value
  would live for years. And a tombstone purged on the grace window alone lets a replica that never
  saw the delete copy its older value back, which is permanent: no tombstone is left to beat it.
-/
import Ferrosa.Ledger

set_option linter.deprecated false

namespace Ferrosa.Storage.Tombstone

open Ferrosa.Ledger

abbrev Val := Nat

/-! ## The observable cell

A cell is reduced to what a reader can see: a write timestamp, whether it is a tombstone, and a value. The
local deletion time and the TTL are deliberately absent, because they decide *when* a cell is visible rather
than *which* cell wins — resolution and expiry are separate questions with separate answers. Keeping the
merge on this three-field record is what makes the tie-breaks statable, and what keeps the expiry machinery
out of the conflict rule.
-/

/-- What a reader can see of a cell: the write timestamp, whether it is a tombstone, and
    the value. `ldt` and the TTL are deliberately absent — they change *when* the cell is
    visible, not *which* cell wins. -/
structure Obs where
  ts : Nat
  tb : Bool
  val : Val
  deriving DecidableEq, Repr

/-- Last-write-wins with the two required tie-breaks: newer timestamp, then tombstone over
    value, then the value itself. -/
def Obs.merge (a b : Obs) : Obs :=
  if a.ts > b.ts then a
  else if b.ts > a.ts then b
  else if a.tb && !b.tb then a
  else if b.tb && !a.tb then b
  else if a.val >= b.val then a
  else b

/-- The newer write wins. -/
theorem merge_prefers_newer (a b : Obs) (h : a.ts > b.ts) : a.merge b = a := by
  simp [Obs.merge, h]

/-- **The tombstone wins a timestamp tie.**  Otherwise a delete and an insert carrying the
    same timestamp would resolve by whichever arrived last. -/
theorem merge_tombstone_wins_tie (a b : Obs) (hts : a.ts = b.ts)
    (ha : a.tb = true) (hb : b.tb = false) : a.merge b = a := by
  simp [Obs.merge, hts, ha, hb]

/-- Merging a cell with itself is that cell — the property replay depends on. -/
theorem merge_idempotent (a : Obs) : a.merge a = a := by
  cases a with
  | mk ts tb val => simp [Obs.merge]

/-- A delete hides the row. -/
def RowState := Option Obs

def RowState.visible : RowState → Bool
  | none => false
  | some o => !o.tb

def RowState.applyCell (s : RowState) (c : Obs) : RowState :=
  match s with
  | none => some c
  | some x => some (x.merge c)

/-! ## The tie-breaks are requirements, not tastes

Two merges that omit a tie-break, and the concrete states that expose each. -/

/-- A merge that resolves on the timestamp alone. -/
def mergeTsOnly (a b : Obs) : Obs := if a.ts > b.ts then a else b

/-- Timestamp-only merge is still idempotent, so replay is not where the missing tie-break shows up:
    the failure appears between replicas, each of which keeps its own second argument and never
    converges with the other. -/
theorem mergeTsOnly_idempotent (a : Obs) : mergeTsOnly a a = a := by simp [mergeTsOnly]

/-- **Without a value tie-break, two replicas keep different winners.**  `USING TIMESTAMP`
    lets two clients write the same timestamp, so this is reachable, and the replicas never
    converge because each merge keeps its own second argument. -/
theorem ts_only_merge_does_not_converge :
    mergeTsOnly ⟨5, false, 1⟩ ⟨5, false, 2⟩ ≠ mergeTsOnly ⟨5, false, 2⟩ ⟨5, false, 1⟩ := by
  decide

/-- A merge where the value beats the tombstone on a tie. -/
def mergeValueWins (a b : Obs) : Obs :=
  if a.ts > b.ts then a
  else if b.ts > a.ts then b
  else if a.val >= b.val then a
  else b

/-- **Without the tombstone tie-break, a delete is silently ignored.**  A delete and an
    insert carrying the same timestamp resolve deterministically to the insert, so both
    replicas agree — and both are wrong: the row the user deleted is still there. Convergence
    without the tombstone rule buys nothing. -/
theorem value_wins_ignores_a_delete :
    mergeValueWins ⟨5, true, 0⟩ ⟨5, false, 9⟩ = ⟨5, false, 9⟩
      ∧ mergeValueWins ⟨5, false, 9⟩ ⟨5, true, 0⟩ = ⟨5, false, 9⟩ := by
  decide

/-- The tombstone-aware merge resolves the same pair to the delete, in both orders. -/
theorem tombstone_tiebreak_resolves_a_delete :
    Obs.merge ⟨5, true, 0⟩ ⟨5, false, 9⟩ = ⟨5, true, 0⟩
      ∧ Obs.merge ⟨5, false, 9⟩ ⟨5, true, 0⟩ = ⟨5, true, 0⟩ := by
  decide

/-- With both tie-breaks the merge is commutative on the concrete colliding cases, so
    replicas agree whichever order they merge in. -/
theorem merge_converges_on_collisions :
    Obs.merge ⟨5, true, 0⟩ ⟨5, false, 9⟩ = Obs.merge ⟨5, false, 9⟩ ⟨5, true, 0⟩
      ∧ Obs.merge ⟨5, false, 1⟩ ⟨5, false, 2⟩ = Obs.merge ⟨5, false, 2⟩ ⟨5, false, 1⟩ := by
  decide

/-! ## TTL

A local deletion time is computed once, at write time, from the write timestamp, and stored with the cell;
expiry is then a comparison of the reader's clock against that stored number. A replica never recomputes it,
so a wrong clock on one node cannot make the same cell expire at a different moment for different readers.
The number that goes on the wire is the number every node compares against, which is why a cell with a TTL
disappears at its deletion second across the cluster rather than per replica.
-/

/-- A cell written with a TTL: the local deletion time is computed once, at write time,
    from the write timestamp. `ttl_ldt` is the value that goes on the wire. -/
def ttlLdt (writeTs ttl : Nat) : Nat := writeTs + ttl

/-- Expiry is a comparison against the stored number. -/
def expiredAt (now ldt : Nat) : Bool := now >= ldt

/-- **The stored number is what decides**, so a replica's clock never enters the answer:
    two readers at the same instant agree even if one clock is wrong, because neither
    recomputes `ldt`. -/
theorem expiry_depends_only_on_the_stored_ldt (now a b : Nat) (h : a = b) :
    expiredAt now a = expiredAt now b := by rw [h]

/-- The same cell read at two instants: visible before `ldt`, gone at it. -/
theorem ttl_expires_exactly_at_ldt (writeTs ttl : Nat) (httl : 0 < ttl) :
    expiredAt (ttlLdt writeTs ttl - 1) (ttlLdt writeTs ttl) = false
      ∧ expiredAt (ttlLdt writeTs ttl) (ttlLdt writeTs ttl) = true := by
  constructor <;> simp [expiredAt, ttlLdt] <;> omega

/-- **A write with a past `USING TIMESTAMP` and a TTL is invisible on arrival.**  The
    coordinator computes `ldt` from the supplied timestamp, so a client clock (or a replay
    of an old mutation) can produce a cell that is already expired when it is stored. -/
theorem past_timestamp_expires_on_arrival :
    expiredAt 5000 (ttlLdt 1000 10) = true := by decide

/-- Seconds since the epoch, stored in an `i32` for a cell's local deletion time. -/
def toI32 (n : Nat) : Int :=
  let m := n % 4294967296
  if m < 2147483648 then (m : Int) else (m : Int) - 4294967296

/-- Expiry as the reader sees it, on the signed field. -/
def expiredSigned (now ldt : Int) : Bool := now >= ldt

/-- **The `i32` local-deletion-time wraps past 2038.**  A cell written today with a TTL
    that reaches past `2^31` seconds has a negative stored `ldt`, and a negative `ldt`
    compares as already expired — so the value is dropped at read time while the writer was
    told it would live for years. A twelve-year TTL is enough. -/
theorem ldt_overflows_before_2038_reachable_now :
    toI32 2147483648 = -2147483648
      ∧ toI32 (1800000000 + 500000000) < 0
      ∧ expiredSigned 1800000000 (toI32 (1800000000 + 500000000)) = true := by
  decide

/-! ## Purge and resurrection

A tombstone can be discarded once the grace window has elapsed and every replica holds it, and discarding it
earlier can undo the delete permanently. The no-purge state is safe and unbounded — absence is monotone
because the tombstone keeps winning against any value it dominates — so purging is what bounds storage, at
the price of a precondition that has to hold on every replica rather than only on the clock. Getting that
precondition wrong has one consequence: a value a replica never deleted comes back on the next read repair,
and no tombstone is left to beat it.
-/

/-- Discard a tombstone that is older than the grace period. -/
def RowState.purgeOldTombstone (now grace : Nat) : RowState → RowState
  | none => none
  | some o => if o.tb && now >= o.ts + grace then none else some o

/-- **With no purge, absence is monotone**: a merged tombstone stays the winner against any
    value it dominates, so deleted data cannot come back. This is the current state of the
    tree, and it is correct and unbounded. -/
theorem no_purge_preserves_absence (v : Obs) (h : v.ts < 5) :
    (RowState.applyCell (some ⟨5, true, 0⟩) v).visible = false := by
  cases v with
  | mk ts tb val => simp [RowState.applyCell, RowState.visible, Obs.merge, h]

/-- **Purging an unpropagated tombstone resurrects the value.**  Replica A takes the delete
    and purges at grace; replica B never saw the tombstone and still holds the older value.
    The next read-repair copies B's value back and the delete is undone permanently, because
    no tombstone remains to beat it. -/
def resurrectAfterPurge : Bool :=
  let a : RowState := some ⟨5, true, 0⟩ -- A: the delete
  let b : RowState := some ⟨3, false, 7⟩ -- B: never saw it
  let a' := RowState.purgeOldTombstone 100 10 a -- grace elapsed on A
  let a'' := a'.applyCell (b.getD ⟨0, false, 0⟩) -- read repair pulls B's value back
  a''.visible

/-- Replica A purges its tombstone when grace elapses while replica B never saw the delete; read
    repair then pulls B's older value back and the row is visible again, with no tombstone remaining
    to hide it. Purging on time alone is how a deleted row comes back for good. -/
theorem purge_before_propagation_resurrects : resurrectAfterPurge = true := by decide

/-- **Purging is safe once every replica has applied the tombstone.**  Then no replica holds
    an older value to resurrect, and the merge after the purge cannot produce a visible cell. -/
theorem purge_safe_after_all_replicas_applied (s : Obs) (h : s.tb = true) :
    (RowState.applyCell (RowState.purgeOldTombstone 100 10 (some s)) s).visible = false := by
  by_cases hp : s.ts ≤ 90
  · simp [RowState.purgeOldTombstone, RowState.applyCell, RowState.visible, h, hp]
  · simp [RowState.purgeOldTombstone, RowState.applyCell, RowState.visible, h, hp,
      merge_idempotent]

/-- The premise the grace period stands in for, named so it can be refused: every replica
    has contacted the coordinator (and so applied the tombstone) within the grace window. -/
def AllReplicasApplied (replicas : List Obs) (tomb : Obs) : Prop :=
  ∀ r ∈ replicas, r.tb = true ∨ r.ts > tomb.ts

/-- **Grace is a proxy for that premise, and only that.**  If a replica still holds an older
    value, discarding the tombstone is unsafe — `purge_before_propagation_resurrects` is the
    witness. -/
theorem purge_requires_all_replicas_applied (old : Obs) (tomb : Obs) (h : old.ts < tomb.ts)
    (hvis : old.tb = false) : ¬ AllReplicasApplied [old] tomb := by
  intro hall
  have := hall old (by simp)
  rcases this with htb | hlt
  · rw [hvis] at htb; exact Bool.noConfusion htb
  · omega

/-! ### Resurrection is an ordering bug, and the purge precondition is what forbids it

A tombstone shadows every value at or below its timestamp whichever arrives first, so the only way deleted
data returns is for the tombstone to be gone before a replica that still holds the value is read. That makes
resurrection an ordering failure rather than a lost update, and it is why purge waits for both conditions at
once: the grace window elapsed and every replica holding the tombstone. With no purge at all the property
holds for free; the red item is the state where `gc_grace` is consulted alone, which is what an operator gets
if the purge path checks the clock and nothing else.
-/

/-- **A tombstone shadows every value at or below its timestamp**, whoever arrives first. -/
theorem a_tombstone_shadows_every_value_at_or_below_its_timestamp
    (ts tsv v : Nat) (h : tsv ≤ ts) :
    (⟨ts, true, 0⟩ : Obs).merge ⟨tsv, false, v⟩ = ⟨ts, true, 0⟩
      ∧ (⟨tsv, false, v⟩ : Obs).merge ⟨ts, true, 0⟩ = ⟨ts, true, 0⟩ := by
  rcases Nat.eq_or_lt_of_le h with heq | hlt
  · subst heq
    exact ⟨merge_tombstone_wins_tie _ _ rfl rfl rfl, by simp [Obs.merge]⟩
  · refine ⟨merge_prefers_newer _ _ hlt, ?_⟩
    have h1 : ¬ (tsv > ts) := by omega
    simp [Obs.merge, h1, hlt]

/-- `gc_grace` elapsed **and** every replica holds the tombstone. -/
def safeToPurge (now wroteAt gcGrace : Nat) (allReplicasHaveIt : Bool) : Bool :=
  allReplicasHaveIt && decide (wroteAt + gcGrace ≤ now)

/-- **Purging needs both conditions.** -/
theorem purging_needs_the_grace_window_and_propagation :
    safeToPurge 100 50 30 true = true
      ∧ safeToPurge 79 50 30 true = false
      ∧ safeToPurge 100 50 30 false = false := by decide

/-- **Purging before propagation resurrects.** -/
theorem purging_before_propagation_lets_a_stale_value_win :
    ([] : List Obs) ++ [⟨50, false, 7⟩] = [⟨50, false, 7⟩]
      ∧ (⟨50, false, 7⟩ : Obs).merge ⟨50, false, 7⟩ = ⟨50, false, 7⟩ := by decide

/-! ## Expired TTL data must leave at compaction, and only where nothing older can sit under it

An expired TTL cell is invisible to every reader, yet it still has a value, so a purge that drops only
value-less markers keeps it forever. A partition that receives a TTL'd row per event (a session listing,
a presence log) then grows with every event ever written: each read merges, digests and ships the whole
history to throw it away at the coordinator. That was the shipped rule (`purge_row` dropped a cell only
when `is_tombstone()`), and `expired_history_grows_without_bound` is its witness.

The repair drops an expired cell under the same two conditions as a tombstone: its stored local deletion
time is past `gc_before`, and its timestamp is below the overlap guard, the smallest timestamp of any data
outside the compaction. Both conditions are needed. Below the guard, every other version of the cell is
strictly newer and wins the merge anyway, so the read cannot change (`purging_expired_data_is_invisible_to_reads`).
Without the guard, an older value outside the compaction (or on a replica that never saw the TTL write)
is no longer shadowed and comes back (`dropping_expired_data_above_the_guard_resurrects`).

Rewriting the expired cell as a tombstone, which is how Cassandra spells it, is not equivalent here: the
reconcile order is expiry-blind and breaks an equal-timestamp tie on the value, so an expired cell loses
to a larger live value while a tombstone at that timestamp beats it.
`rewriting_expired_data_as_a_tombstone_changes_a_tie` is the witness, and it is why the repair drops the
cell outright rather than rewriting it.
-/

/-- A stored cell: what a reader can see, plus its stored local deletion time. A tombstone's is its
    deletion second, an expiring cell's is its expiry second, and a cell that never expires has
    none (`i32::MAX` in Rust). -/
structure Stored where
  obs : Obs
  ldt : Option Nat
  deriving DecidableEq, Repr

/-- `row_is_visible` in `ferrosa-row-bridge`: not a tombstone, and not past its expiry. -/
def Stored.visibleAt (now : Nat) (c : Stored) : Bool :=
  !c.obs.tb && (match c.ldt with
    | none => true
    | some l => !expiredAt now l)

/-- The reconcile winner of two versions of one cell (`reconcile_cmp`): newer timestamp first,
    then the tombstone/value tie-breaks of `Obs.merge`. -/
def Stored.merge (a b : Stored) : Stored :=
  if a.obs.ts > b.obs.ts then a
  else if b.obs.ts > a.obs.ts then b
  else if a.obs.merge b.obs = a.obs then a
  else b

/-- A read of one cell: fold every stored version into the winner, then ask whether it is visible. -/
def readAt (now : Nat) (versions : List Stored) : Bool :=
  match versions.foldl (fun acc c => some (match acc with
      | none => c
      | some x => x.merge c)) none with
  | none => false
  | some w => w.visibleAt now

/-- Past the grace window and below the overlap guard (`PurgePolicy::purgeable`). -/
def purgeableAt (gcBefore maxTs : Nat) (c : Stored) : Bool :=
  (match c.ldt with
    | none => false
    | some l => decide (l < gcBefore)) && decide (c.obs.ts < maxTs)

/-- The shipped rule: only a value-less cell may be dropped. -/
def droppableShipped (gcBefore maxTs : Nat) (c : Stored) : Bool :=
  c.obs.tb && purgeableAt gcBefore maxTs c

/-- The repaired rule: a tombstone or an expiring cell, under the same two conditions. -/
def droppableRepaired (gcBefore maxTs : Nat) (c : Stored) : Bool :=
  (c.obs.tb || c.ldt.isSome) && purgeableAt gcBefore maxTs c

/-- Compaction output for cells that each sit in their own row: keep what may not be dropped. -/
def compactCells (drop : Stored → Bool) (cells : List Stored) : List Stored :=
  cells.filter (fun c => !drop c)

/-- `n` TTL'd rows, one per event, each written at second `i` and expiring at `i + 1`. -/
def ttlHistory (n : Nat) : List Stored :=
  (List.range n).map (fun i => ⟨⟨i, false, 1⟩, some (i + 1)⟩)

/-- **The shipped purge never drops an expired value.** Whatever the clock and the guard say. -/
theorem shipped_purge_never_drops_an_expired_value (gcBefore maxTs : Nat) (c : Stored)
    (h : c.obs.tb = false) : droppableShipped gcBefore maxTs c = false := by
  simp [droppableShipped, h]

/-- **The bug: expired TTL history grows without bound.** Every one of the `n` rows is invisible
    long before the compaction runs, yet the shipped rule keeps all `n` of them, for any grace
    window and any guard. -/
theorem expired_history_grows_without_bound (n gcBefore maxTs : Nat) :
    (∀ c ∈ ttlHistory n, c.visibleAt (n + 1) = false)
      ∧ (compactCells (droppableShipped gcBefore maxTs) (ttlHistory n)).length = n := by
  constructor
  · intro c hc
    simp only [ttlHistory, List.mem_map, List.mem_range] at hc
    obtain ⟨i, hi, rfl⟩ := hc
    simp [Stored.visibleAt, expiredAt]
    omega
  · have hkeep : ∀ c ∈ ttlHistory n, (!droppableShipped gcBefore maxTs c) = true := by
      intro c hc
      simp only [ttlHistory, List.mem_map, List.mem_range] at hc
      obtain ⟨i, _, rfl⟩ := hc
      simp [droppableShipped]
    rw [compactCells, List.filter_eq_self.mpr hkeep]
    simp [ttlHistory]

/-- **The repair bounds it.** Once the grace window and the guard have both passed the history,
    a compaction removes every expired row. -/
theorem repaired_compaction_drops_expired_history (n : Nat) :
    compactCells (droppableRepaired (n + 1) (n + 1)) (ttlHistory n) = [] := by
  simp only [compactCells, ttlHistory, List.filter_map, List.map_eq_nil_iff,
    List.filter_eq_nil_iff, List.mem_range, Function.comp, droppableRepaired, purgeableAt]
  intro i hi
  simp
  omega

/-- A version strictly older than another loses the merge, whichever side it is on. -/
theorem stored_merge_older_loses (a b : Stored) (h : a.obs.ts < b.obs.ts) :
    a.merge b = b := by
  have h1 : ¬ (a.obs.ts > b.obs.ts) := by omega
  simp [Stored.merge, h1, h]

/-- A droppable cell is invisible from the moment its grace window has passed. -/
theorem droppable_cell_is_invisible (now gcBefore maxTs : Nat) (c : Stored)
    (hd : droppableRepaired gcBefore maxTs c = true) (hnow : gcBefore ≤ now) :
    c.visibleAt now = false := by
  rcases c with ⟨⟨ts, tb, val⟩, ldt⟩
  cases ldt with
  | none => simp [droppableRepaired, purgeableAt] at hd
  | some l =>
    simp [droppableRepaired, purgeableAt] at hd
    cases tb <;> simp [Stored.visibleAt, expiredAt] <;> omega

/-- **Correctness: dropping a cell the repaired rule allows is invisible to every later read.**
    `others` is every other version of the cell that survives anywhere: outside this compaction
    (in another SSTable or a memtable), all at or above the overlap guard. Reading with the dropped
    cell and reading without it give the same answer at every instant past the grace window. This
    covers tombstones and expired TTL cells alike. -/
theorem purging_expired_data_is_invisible_to_reads
    (now gcBefore maxTs : Nat) (c : Stored) (others : List Stored)
    (hd : droppableRepaired gcBefore maxTs c = true) (hnow : gcBefore ≤ now)
    (hguard : ∀ o ∈ others, maxTs ≤ o.obs.ts) :
    readAt now (c :: others) = readAt now others := by
  have hts : c.obs.ts < maxTs := by
    rcases c with ⟨⟨ts, tb, val⟩, ldt⟩
    show ts < maxTs
    cases ldt <;> simp [droppableRepaired, purgeableAt] at hd <;> omega
  cases others with
  | nil =>
    simp [readAt, droppable_cell_is_invisible now gcBefore maxTs c hd hnow]
  | cons o rest =>
    have hlt : c.obs.ts < o.obs.ts := Nat.lt_of_lt_of_le hts (hguard o (by simp))
    simp [readAt, List.foldl, stored_merge_older_loses c o hlt]

/-- **The guard is required.** An expired cell written at 10 shadows an older value written at 5
    that lives outside the compaction (or on a replica that never saw the TTL write). Dropping the
    expired cell brings the old value back. -/
theorem dropping_expired_data_above_the_guard_resurrects :
    readAt 100 [⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨5, false, 7⟩, none⟩] = false
      ∧ readAt 100 [⟨⟨5, false, 7⟩, none⟩] = true := by
  decide

/-- **Rewriting an expired cell as a tombstone is not a no-op.** At an equal timestamp the
    expiry-blind reconcile picks the larger live value over the expired one, so the read sees it;
    a tombstone at that timestamp beats it, so the read would not. -/
theorem rewriting_expired_data_as_a_tombstone_changes_a_tie :
    readAt 100 [⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨10, false, 9⟩, none⟩] = true
      ∧ readAt 100 [⟨⟨10, true, 0⟩, some 20⟩, ⟨⟨10, false, 9⟩, none⟩] = false := by
  decide

/-- **Replicas that differ only by purgeable data agree once it is stripped.** Replicas purge on
    their own compaction schedules, so one can still hold expired or deleted data another has
    dropped. A digest over the raw cells called that a divergence and read repair wrote the
    purged data back. The read digest is taken over the cells that survive the repaired rule, with
    the guard open (a read has merged every local source; Rust passes `i64::MAX` as `openGuard`), so
    the two replicas hash the same: the cells only one of them still holds all leave. -/
theorem replicas_differing_only_by_purgeable_data_agree
    (gcBefore openGuard : Nat) (shared extra : List Stored)
    (hextra : ∀ c ∈ extra, droppableRepaired gcBefore openGuard c = true) :
    compactCells (droppableRepaired gcBefore openGuard) (shared ++ extra)
      = compactCells (droppableRepaired gcBefore openGuard) shared := by
  have hnone : compactCells (droppableRepaired gcBefore openGuard) extra = [] := by
    simp only [compactCells, List.filter_eq_nil_iff]
    intro c hc
    rw [hextra c hc]
    decide
  simp only [compactCells, List.filter_append] at hnone ⊢
  rw [hnone, List.append_nil]

/-! ## Read repair

When the responders' digests differ, the coordinator merges every responder's copy and writes the
merged winner back. The shipped write sent the winner to every responder whatever it was. A winner past
its grace window (an expired TTL cell, or an old tombstone that one replica had already compacted away)
went back to the replicas that had dropped it. No read could see it there, yet each target stored it
until its next compaction (`shipped_repair_writes_purged_data_back`).

The repaired write treats the target the way a compaction treats data outside it. The target's guard
is the oldest timestamp of anything it holds that is not past grace (`replicaGuard`). A winner that is
past grace and below that guard is not sent. Everything the target holds is then either newer than the
winner, which is impossible because the winner won, or itself past grace. So the target reads the
merged answer either way (`repair_converges_every_replica_to_the_merged_answer`), and the target's own
compaction would have dropped the winner anyway
(`repaired_write_matches_the_shipped_write_after_compaction`). A winner that shadows a live, older value
on the target is still sent, because without it that value comes back
(`skipping_a_repair_over_older_live_data_resurrects`).

The coordinator holds the data of the full-read replica and of each replica it refetched, but not of
replicas whose digest matched. A matching digest means the two replicas agree on everything that is
not past grace, so they share a guard (`replicas_with_equal_digests_share_the_guard`), and the
full-read replica's guard stands in for theirs.
-/

/-- Past the grace window, whatever the guard: a tombstone or expiring cell whose stored local deletion
    time is before `gcBefore`. A cell that never expires has none, so it never is. -/
def pastGrace (gcBefore : Nat) (c : Stored) : Bool :=
  match c.ldt with
  | none => false
  | some l => decide (l < gcBefore)

/-- The repaired rule, split into its two conditions. -/
theorem droppable_iff_past_grace_below_guard (gcBefore maxTs : Nat) (c : Stored) :
    droppableRepaired gcBefore maxTs c = (pastGrace gcBefore c && decide (c.obs.ts < maxTs)) := by
  rcases c with ⟨⟨ts, tb, val⟩, ldt⟩
  cases ldt <;> cases tb <;> simp [droppableRepaired, purgeableAt, pastGrace]

/-- A droppable cell sits below the guard. -/
theorem ts_lt_guard_of_droppable (gcBefore maxTs : Nat) (c : Stored)
    (hd : droppableRepaired gcBefore maxTs c = true) : c.obs.ts < maxTs := by
  rw [droppable_iff_past_grace_below_guard] at hd
  simp at hd
  exact hd.2

/-- A cell past grace is invisible from `gcBefore` on, whatever its timestamp. -/
theorem past_grace_is_invisible (now gcBefore : Nat) (c : Stored)
    (hp : pastGrace gcBefore c = true) (hnow : gcBefore ≤ now) : c.visibleAt now = false := by
  rcases c with ⟨⟨ts, tb, val⟩, ldt⟩
  cases ldt with
  | none => simp [pastGrace] at hp
  | some l =>
    simp [pastGrace] at hp
    cases tb <;> simp [Stored.visibleAt, expiredAt] <;> omega

/-- One step of the read fold. -/
def readStep (acc : Option Stored) (c : Stored) : Option Stored :=
  some (match acc with
    | none => c
    | some x => x.merge c)

/-- The version a read of `vs` resolves to, if any. -/
def winner (vs : List Stored) : Option Stored := vs.foldl readStep none

/-- A read is its winner's visibility, or nothing when there is no version. -/
theorem readAt_eq_winner (now : Nat) (vs : List Stored) :
    readAt now vs = (match winner vs with
      | none => false
      | some w => w.visibleAt now) := rfl

/-- The merge picks one of its two arguments. -/
theorem stored_merge_is_an_argument (a b : Stored) : a.merge b = a ∨ a.merge b = b := by
  unfold Stored.merge
  by_cases h1 : a.obs.ts > b.obs.ts
  · simp [h1]
  · by_cases h2 : b.obs.ts > a.obs.ts
    · simp [h1, h2]
    · by_cases h3 : a.obs.merge b.obs = a.obs
      · simp [h1, h2, h3]
      · simp [h1, h2, h3]

/-- A fold either keeps its seed or lands on a member of the list. -/
theorem fold_lands_on_seed_or_member (vs : List Stored) (acc : Option Stored) (w : Stored)
    (h : vs.foldl readStep acc = some w) : acc = some w ∨ w ∈ vs := by
  induction vs generalizing acc with
  | nil => left; simpa using h
  | cons c rest ih =>
    simp only [List.foldl] at h
    rcases ih (readStep acc c) h with hstep | hmem
    · cases acc with
      | none =>
        simp [readStep] at hstep
        right; simp [hstep]
      | some x =>
        simp [readStep] at hstep
        rcases stored_merge_is_an_argument x c with hx | hc
        · left; rw [← hstep, hx]
        · right; rw [← hstep, hc]; simp
    · right; simp [hmem]

/-- A read resolves to one of the versions it read. -/
theorem winner_is_a_member (vs : List Stored) (w : Stored) (h : winner vs = some w) : w ∈ vs := by
  rcases fold_lands_on_seed_or_member vs none w h with h0 | hm
  · cases h0
  · exact hm

/-- A seed that beats every version stays the winner. -/
theorem fold_keeps_a_dominating_seed (w : Stored) (vs : List Stored)
    (hwin : ∀ v ∈ vs, w.merge v = w) : vs.foldl readStep (some w) = some w := by
  induction vs with
  | nil => rfl
  | cons v rest ih =>
    simp only [List.foldl, readStep]
    rw [hwin v (by simp)]
    exact ih (fun u hu => hwin u (by simp [hu]))

/-- A replica's guard: the oldest timestamp of anything it holds that is not past grace, or `top`
    when it holds nothing else (Rust: `i64::MAX`). The compaction overlap guard, with the target
    replica as the data outside. -/
def replicaGuard (gcBefore top : Nat) : List Stored → Nat
  | [] => top
  | v :: rest =>
    if pastGrace gcBefore v then replicaGuard gcBefore top rest
    else min v.obs.ts (replicaGuard gcBefore top rest)

/-- Everything a replica holds is past grace or at or above its guard. -/
theorem replicaGuard_bounds (gcBefore top : Nat) (r : List Stored) :
    ∀ v ∈ r, pastGrace gcBefore v = true ∨ replicaGuard gcBefore top r ≤ v.obs.ts := by
  induction r with
  | nil => simp
  | cons v rest ih =>
    intro u hu
    simp only [List.mem_cons] at hu
    by_cases hp : pastGrace gcBefore v = true
    · have hg : replicaGuard gcBefore top (v :: rest) = replicaGuard gcBefore top rest := by
        simp [replicaGuard, hp]
      rw [hg]
      rcases hu with rfl | hu
      · left; exact hp
      · exact ih u hu
    · have hg : replicaGuard gcBefore top (v :: rest)
          = min v.obs.ts (replicaGuard gcBefore top rest) := by
        simp [replicaGuard, hp]
      rw [hg]
      rcases hu with rfl | hu
      · right; omega
      · rcases ih u hu with h | h
        · left; exact h
        · right; omega

/-- The shipped repair: the merged winner goes to every responder. -/
def repairShipped (w : Stored) (r : List Stored) : List Stored := w :: r

/-- The repaired write to one target: the merged winner, unless it is past grace and below the
    target's guard. -/
def repairWrite (gcBefore top : Nat) (w : Stored) (r : List Stored) : List Stored :=
  if droppableRepaired gcBefore (replicaGuard gcBefore top r) w then [] else [w]

/-- **The bug: the shipped repair writes purged data back.** One replica already compacted an expired
    TTL cell away and another still holds it, so the merged winner is that expired cell. The shipped
    repair stores it on the replica that had dropped it, though no read can see it there. The
    repaired write sends nothing. -/
theorem shipped_repair_writes_purged_data_back :
    readAt 100 [] = false
      ∧ readAt 100 (repairShipped ⟨⟨10, false, 1⟩, some 20⟩ []) = false
      ∧ (repairShipped ⟨⟨10, false, 1⟩, some 20⟩ []).length = 1
      ∧ repairWrite 30 1000 ⟨⟨10, false, 1⟩, some 20⟩ [] = [] := by
  decide

/-- **Correctness: every repaired replica reads the coordinator's merged answer.** `w` is the merged
    winner, so it beats every version the target holds. Whether the repaired write sends `w` or not,
    the target then reads what the coordinator returned, at every instant past the grace window. -/
theorem repair_converges_every_replica_to_the_merged_answer
    (now gcBefore top : Nat) (w : Stored) (r : List Stored)
    (hnow : gcBefore ≤ now) (hwin : ∀ v ∈ r, w.merge v = w) :
    readAt now (repairWrite gcBefore top w r ++ r) = w.visibleAt now := by
  unfold repairWrite
  by_cases hd : droppableRepaired gcBefore (replicaGuard gcBefore top r) w = true
  · rw [if_pos hd, List.nil_append, droppable_cell_is_invisible now gcBefore _ w hd hnow]
    have hall : ∀ v ∈ r, pastGrace gcBefore v = true := by
      intro v hv
      rcases replicaGuard_bounds gcBefore top r v hv with h | h
      · exact h
      · exfalso
        have hlt : w.obs.ts < v.obs.ts :=
          Nat.lt_of_lt_of_le (ts_lt_guard_of_droppable gcBefore _ w hd) h
        have hloses := stored_merge_older_loses w v hlt
        rw [hwin v hv] at hloses
        subst hloses
        omega
    rw [readAt_eq_winner]
    cases hw : winner r with
    | none => rfl
    | some u => exact past_grace_is_invisible now gcBefore u (hall u (winner_is_a_member r u hw)) hnow
  · rw [if_neg hd, readAt_eq_winner]
    have hfold : winner ([w] ++ r) = some w := by
      show (w :: r).foldl readStep none = some w
      simp only [List.foldl, readStep]
      exact fold_keeps_a_dominating_seed w r hwin
    rw [hfold]

/-- **Leaving the write out only does early what the target's compaction does.** A compaction of
    the target, no more aggressive than its own guard allows, leaves the same cells after the
    repaired write as after the shipped one. -/
theorem repaired_write_matches_the_shipped_write_after_compaction
    (gcBefore top : Nat) (w : Stored) (r : List Stored) :
    compactCells (droppableRepaired gcBefore (replicaGuard gcBefore top r))
        (repairWrite gcBefore top w r ++ r)
      = compactCells (droppableRepaired gcBefore (replicaGuard gcBefore top r))
          (repairShipped w r) := by
  unfold repairWrite repairShipped compactCells
  by_cases hd : droppableRepaired gcBefore (replicaGuard gcBefore top r) w = true
  · simp [hd]
  · simp [hd]

/-- **The guard is required.** The target still holds a live value written at 5 that the merged
    expired cell (written at 10) shadows. Leaving the write out brings that value back. The guard
    sees the live value, so the repaired write sends the cell. -/
theorem skipping_a_repair_over_older_live_data_resurrects :
    readAt 100 (repairShipped ⟨⟨10, false, 1⟩, some 20⟩ [⟨⟨5, false, 7⟩, none⟩]) = false
      ∧ readAt 100 [⟨⟨5, false, 7⟩, none⟩] = true
      ∧ repairWrite 30 1000 ⟨⟨10, false, 1⟩, some 20⟩ [⟨⟨5, false, 7⟩, none⟩]
          = [⟨⟨10, false, 1⟩, some 20⟩] := by
  decide

/-- The guard ignores what is past grace. -/
theorem replicaGuard_ignores_past_grace (gcBefore top : Nat) (r : List Stored) :
    replicaGuard gcBefore top r
      = replicaGuard gcBefore top (r.filter (fun v => !pastGrace gcBefore v)) := by
  induction r with
  | nil => rfl
  | cons v rest ih =>
    by_cases hp : pastGrace gcBefore v = true
    · simp [replicaGuard, hp, ih]
    · simp [replicaGuard, hp, ih]

/-- With the guard open, the read-digest view keeps exactly what is not past grace. -/
theorem open_view_keeps_what_is_not_past_grace (gcBefore openGuard : Nat) (r : List Stored)
    (hopen : ∀ v ∈ r, v.obs.ts < openGuard) :
    compactCells (droppableRepaired gcBefore openGuard) r
      = r.filter (fun v => !pastGrace gcBefore v) := by
  unfold compactCells
  apply List.filter_congr
  intro v hv
  rw [droppable_iff_past_grace_below_guard]
  simp [hopen v hv]

/-- **Replicas whose read digests match share a guard.** The read digest hashes the open-guard view,
    so matching digests mean matching views, and the guard reads nothing else. The full-read replica's
    guard is therefore the guard of every replica that matched it. -/
theorem replicas_with_equal_digests_share_the_guard
    (gcBefore openGuard top : Nat) (r1 r2 : List Stored)
    (hopen1 : ∀ v ∈ r1, v.obs.ts < openGuard) (hopen2 : ∀ v ∈ r2, v.obs.ts < openGuard)
    (hview : compactCells (droppableRepaired gcBefore openGuard) r1
      = compactCells (droppableRepaired gcBefore openGuard) r2) :
    replicaGuard gcBefore top r1 = replicaGuard gcBefore top r2 := by
  rw [open_view_keeps_what_is_not_past_grace gcBefore openGuard r1 hopen1,
    open_view_keeps_what_is_not_past_grace gcBefore openGuard r2 hopen2] at hview
  rw [replicaGuard_ignores_past_grace, hview, ← replicaGuard_ignores_past_grace]

/-! ## Replica read responses

The replica that answers the full read of a quorum read shipped every version it held. A partition of
expired TTL history went over the wire on every read, though no read could see any of it
(`shipped_response_carries_purged_data`). The repaired response leaves out what the replica's own
compaction would drop: what is past grace and below the replica's own guard (`strippedResponse`). That
removes only data past grace, so the coordinator computes the same repair guard for the replica from
either response (`stripping_keeps_the_replica_guard`).

A version the replica leaves out can still matter to the coordinator's merge, when another responder
holds an older live value under it. Leaving it out brings that value back
(`keeping_a_stripped_response_over_older_live_data_resurrects`). So the replica reports the newest
timestamp it left out, and the coordinator keeps the stripped response only when that timestamp is
below the guard of every other responder it merges. Otherwise it reads the replica again in full.
Under that check the merged answer is the same with the left-out version or without it
(`a_stripped_version_below_every_other_guard_leaves_the_read_unchanged`). When every digest matched,
nothing is merged, and the check holds anyway: replicas that match share the guard the left-out
versions were already below (`a_matching_digest_passes_the_stripped_response_check`).
-/

/-- The response a replica sends under guard `guard`: what its own compaction would keep. Rust passes
    the replica's own `repair_guard`. -/
def strippedResponse (gcBefore guard : Nat) (r : List Stored) : List Stored :=
  compactCells (droppableRepaired gcBefore guard) r

/-- **The bug: the shipped response carries purged data.** A replica holding only an expired TTL cell
    shipped it, though no read can see it. The stripped response is empty. -/
theorem shipped_response_carries_purged_data :
    readAt 100 [⟨⟨10, false, 1⟩, some 20⟩] = false
      ∧ [(⟨⟨10, false, 1⟩, some 20⟩ : Stored)].length = 1
      ∧ strippedResponse 30 (replicaGuard 30 1000 [⟨⟨10, false, 1⟩, some 20⟩])
          [⟨⟨10, false, 1⟩, some 20⟩] = [] := by
  decide

/-- **Stripping keeps the replica's guard.** It removes only what is past grace, and the guard reads
    nothing else, so the coordinator's repair guard for this replica does not depend on which
    response it got. -/
theorem stripping_keeps_the_replica_guard (gcBefore guard top : Nat) (r : List Stored) :
    replicaGuard gcBefore top (strippedResponse gcBefore guard r) = replicaGuard gcBefore top r := by
  rw [replicaGuard_ignores_past_grace gcBefore top (strippedResponse gcBefore guard r),
    replicaGuard_ignores_past_grace gcBefore top r]
  congr 1
  unfold strippedResponse compactCells
  rw [List.filter_filter]
  apply List.filter_congr
  intro v _
  rw [droppable_iff_past_grace_below_guard]
  cases pastGrace gcBefore v <;> simp

/-- A read of versions that are all past grace sees nothing. -/
theorem read_of_past_grace_versions_is_empty (now gcBefore : Nat) (vs : List Stored)
    (hall : ∀ v ∈ vs, pastGrace gcBefore v = true) (hnow : gcBefore ≤ now) :
    readAt now vs = false := by
  rw [readAt_eq_winner]
  cases hw : winner vs with
  | none => rfl
  | some w =>
    exact past_grace_is_invisible now gcBefore w (hall w (winner_is_a_member vs w hw)) hnow

/-- Two folds whose seeds are both at or below `T` agree once the list holds a version newer than `T`:
    the first such version beats either seed and everything before it. -/
theorem fold_forgets_a_low_seed (T : Nat) (vs : List Stored) (a b : Option Stored)
    (ha : ∀ x, a = some x → x.obs.ts ≤ T) (hb : ∀ x, b = some x → x.obs.ts ≤ T)
    (hex : ∃ v ∈ vs, T < v.obs.ts) :
    vs.foldl readStep a = vs.foldl readStep b := by
  induction vs generalizing a b with
  | nil =>
    obtain ⟨v, hv, _⟩ := hex
    simp at hv
  | cons v rest ih =>
    simp only [List.foldl]
    by_cases hv : T < v.obs.ts
    · have hstep : ∀ acc : Option Stored, (∀ x, acc = some x → x.obs.ts ≤ T) →
          readStep acc v = some v := by
        intro acc hacc
        cases acc with
        | none => rfl
        | some x =>
          simp only [readStep]
          rw [stored_merge_older_loses x v (Nat.lt_of_le_of_lt (hacc x rfl) hv)]
      rw [hstep a ha, hstep b hb]
    · have hrest : ∃ u ∈ rest, T < u.obs.ts := by
        obtain ⟨u, hu, hlt⟩ := hex
        simp only [List.mem_cons] at hu
        rcases hu with rfl | hu
        · exact absurd hlt hv
        · exact ⟨u, hu, hlt⟩
      have hlow : ∀ acc : Option Stored, (∀ x, acc = some x → x.obs.ts ≤ T) →
          ∀ x, readStep acc v = some x → x.obs.ts ≤ T := by
        intro acc hacc x hx
        cases acc with
        | none =>
          simp only [readStep, Option.some.injEq] at hx
          subst hx
          omega
        | some y =>
          simp only [readStep, Option.some.injEq] at hx
          rcases stored_merge_is_an_argument y v with h | h
          · rw [h] at hx
            subst hx
            exact hacc y rfl
          · rw [h] at hx
            subst hx
            omega
      exact ih (readStep a v) (readStep b v) (hlow a ha) (hlow b hb) hrest

/-- **Correctness: a left-out version below every other responder's guard changes no read.** `c` is
    the stripped replica's version of a cell, past grace, and `others` is every other responder's
    version of it. When `c` is older than everything live the others hold, the merged read is the same
    with it or without it, at every instant past the grace window. -/
theorem a_stripped_version_below_every_other_guard_leaves_the_read_unchanged
    (now gcBefore top : Nat) (c : Stored) (others : List Stored)
    (hp : pastGrace gcBefore c = true) (hguard : c.obs.ts < replicaGuard gcBefore top others)
    (hnow : gcBefore ≤ now) :
    readAt now (c :: others) = readAt now others := by
  by_cases hlive : ∃ o ∈ others, pastGrace gcBefore o = false
  · obtain ⟨o, ho, hop⟩ := hlive
    have hlt : c.obs.ts < o.obs.ts := by
      rcases replicaGuard_bounds gcBefore top others o ho with h | h
      · rw [hop] at h
        cases h
      · omega
    rw [readAt_eq_winner, readAt_eq_winner]
    have hfold : winner (c :: others) = winner others := by
      show others.foldl readStep (readStep none c) = others.foldl readStep none
      apply fold_forgets_a_low_seed c.obs.ts others
      · intro x hx
        simp only [readStep, Option.some.injEq] at hx
        subst hx
        exact Nat.le_refl _
      · intro x hx
        cases hx
      · exact ⟨o, ho, hlt⟩
    rw [hfold]
  · have hall' : ∀ v ∈ others, pastGrace gcBefore v = true := by
      intro v hv
      cases h : pastGrace gcBefore v
      · exact absurd ⟨v, hv, h⟩ hlive
      · rfl
    have hall : ∀ v ∈ c :: others, pastGrace gcBefore v = true := by
      intro v hv
      simp only [List.mem_cons] at hv
      rcases hv with rfl | hv
      · exact hp
      · exact hall' v hv
    rw [read_of_past_grace_versions_is_empty now gcBefore _ hall hnow,
      read_of_past_grace_versions_is_empty now gcBefore _ hall' hnow]

/-- **The check is required.** The full-read replica left out an expired cell written at 10, and the
    other responder holds a live value written at 5 under it. Merging the stripped response brings that
    value back. The other responder's guard is 5, so the coordinator's check fails and it reads the
    replica again in full. -/
theorem keeping_a_stripped_response_over_older_live_data_resurrects :
    strippedResponse 30 (replicaGuard 30 1000 [⟨⟨10, false, 1⟩, some 20⟩])
        [⟨⟨10, false, 1⟩, some 20⟩] = []
      ∧ readAt 100 [⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨5, false, 7⟩, none⟩] = false
      ∧ readAt 100 [⟨⟨5, false, 7⟩, none⟩] = true
      ∧ ¬ (10 < replicaGuard 30 1000 [⟨⟨5, false, 7⟩, none⟩]) := by
  decide

/-- **A matching digest passes the check.** A version the replica left out sits below its own guard,
    and a replica whose read digest matched shares that guard, so the version sits below the other
    replica's guard too. The fast path that returns the stripped response unmerged is the case the
    check would allow. -/
theorem a_matching_digest_passes_the_stripped_response_check
    (gcBefore openGuard top : Nat) (r1 r2 : List Stored) (c : Stored)
    (hopen1 : ∀ v ∈ r1, v.obs.ts < openGuard) (hopen2 : ∀ v ∈ r2, v.obs.ts < openGuard)
    (hview : compactCells (droppableRepaired gcBefore openGuard) r1
      = compactCells (droppableRepaired gcBefore openGuard) r2)
    (hc : droppableRepaired gcBefore (replicaGuard gcBefore top r1) c = true) :
    c.obs.ts < replicaGuard gcBefore top r2 := by
  rw [← replicas_with_equal_digests_share_the_guard gcBefore openGuard top r1 r2 hopen1 hopen2 hview]
  exact ts_lt_guard_of_droppable gcBefore _ c hc

/-! ## Cassandra's read-path purge, beside Ferrosa's

Checked against Apache Cassandra 5.0 (`cassandra-5.0` branch). `ReadCommand.executeLocally` wraps every
local read, whether data or digest and whether paged or not, in `withoutPurgeableTombstones`. That is a
`PurgeFunction` whose purge evaluator is `time -> true`, so the overlap guard is open and only
`gcBefore` (and `only_purge_repaired_tombstones`) bounds it. A replica merges its own versions of a
cell first and then drops the winner if it is past grace (`cassandraResponse`). On one replica this
changes no read (`cassandra_response_reads_like_the_replica`).

The coordinator merges the stripped responses without any check. That is correct only when no other
replica holds a live value under the dropped version
(`cassandra_read_is_correct_when_nothing_live_sits_under_the_dropped_version`). Cassandra does not
check that. It is what the operator's contract promises: repair runs within `gc_grace_seconds`, so a
deletion or expiring write reached every replica before anything drops it. When repair has not run,
the older value comes back (`cassandra_read_without_propagation_resurrects`). This is Cassandra's
documented zombie-data failure.

Ferrosa checks the same condition on each read instead of assuming it. The replica reports the newest
timestamp it left out. The coordinator merges the stripped response only when that timestamp is below
every other responder's guard, and otherwise reads the replica again in full. The result equals the
full read with no assumption about repair (`ferrosa_read_needs_no_propagation`). On the same inputs
where Cassandra resurrects, Ferrosa returns the deletion.

Cassandra strips paged reads too, because its `DataLimits` counter counts only rows with live data,
so dropping a row no read can see moves no page boundary
(`a_live_row_limit_keeps_its_live_rows_after_stripping`). Ferrosa's internode `row_limit` counts raw
rows, and under it stripping a page returns different rows
(`a_raw_row_limit_moves_when_a_page_is_stripped`). That is why Ferrosa strips only whole-partition
reads. Stripping pages the way Cassandra does needs a limit that counts live rows first.

Each row is modelled as one cell, so a row is visible exactly when its cell is.
-/

/-- Cassandra's read response for one cell: the replica's own merged winner, dropped when it is past
    grace, with no overlap guard (`WithoutPurgeableTombstones`, evaluator `time -> true`). -/
def cassandraResponse (gcBefore : Nat) (r : List Stored) : List Stored :=
  match winner r with
  | none => []
  | some w => if pastGrace gcBefore w then [] else [w]

/-- **On one replica, Cassandra's strip changes no read.** The replica merges before it purges, so a
    dropped winner was invisible already, and a kept one is the read. -/
theorem cassandra_response_reads_like_the_replica (now gcBefore : Nat) (r : List Stored)
    (hnow : gcBefore ≤ now) :
    readAt now (cassandraResponse gcBefore r) = readAt now r := by
  rw [readAt_eq_winner now r]
  unfold cassandraResponse
  cases hw : winner r with
  | none => rfl
  | some w =>
    by_cases hp : pastGrace gcBefore w = true
    · simp only [hp, if_true]
      exact (past_grace_is_invisible now gcBefore w hp hnow).symm
    · simp only [hp]
      rfl

/-- **Cassandra's read is correct when nothing live sits under the dropped version.** `c` is the version
    one replica dropped, and `b` is another replica's. When everything live on `b` is newer than `c`,
    which is what repair within `gc_grace_seconds` promises, the merge of the two stripped responses
    is the full read. -/
theorem cassandra_read_is_correct_when_nothing_live_sits_under_the_dropped_version
    (now gcBefore top : Nat) (c : Stored) (b : List Stored)
    (hp : pastGrace gcBefore c = true) (hguard : c.obs.ts < replicaGuard gcBefore top b)
    (hnow : gcBefore ≤ now) :
    readAt now (cassandraResponse gcBefore [c] ++ cassandraResponse gcBefore b)
      = readAt now (c :: b) := by
  have h1 : cassandraResponse gcBefore [c] = [] := by
    simp [cassandraResponse, winner, readStep, hp]
  rw [h1, List.nil_append, cassandra_response_reads_like_the_replica now gcBefore b hnow,
    a_stripped_version_below_every_other_guard_leaves_the_read_unchanged now gcBefore top c b hp hguard hnow]

/-- Ferrosa's coordinated read of a stripped full-read response `a` and another responder `b`: keep
    the stripped response when everything it left out is below `b`'s guard, otherwise read `a` again
    in full (`gather_quorum_reads` with `PurgedReadRequest`). -/
def ferrosaRead (now gcBefore top : Nat) (a b : List Stored) : Bool :=
  if (a.filter (droppableRepaired gcBefore (replicaGuard gcBefore top a))).all
      (fun c => decide (c.obs.ts < replicaGuard gcBefore top b))
  then readAt now (strippedResponse gcBefore (replicaGuard gcBefore top a) a ++ b)
  else readAt now (a ++ b)

/-- **Without repair, Cassandra's read resurrects and Ferrosa's does not.** One replica holds an
    expiring write at 10 that expired before `gcBefore`. The other never received it and still holds a
    live value written at 5. The full read sees nothing. Cassandra drops the expired write and returns
    the value at 5. Ferrosa's check fails (5 is not above 10), so it reads in full and returns
    nothing. -/
theorem cassandra_read_without_propagation_resurrects :
    readAt 100 [⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨5, false, 7⟩, none⟩] = false
      ∧ readAt 100 (cassandraResponse 30 [⟨⟨10, false, 1⟩, some 20⟩]
          ++ cassandraResponse 30 [⟨⟨5, false, 7⟩, none⟩]) = true
      ∧ ferrosaRead 100 30 1000 [⟨⟨10, false, 1⟩, some 20⟩] [⟨⟨5, false, 7⟩, none⟩] = false := by
  decide

/-- **Ferrosa's checked read needs no propagation.** For any version `c` the full-read replica holds and
    anything `b` holds, the coordinated read equals the full read past the grace window, whether or not
    repair has run. -/
theorem ferrosa_read_needs_no_propagation (now gcBefore top : Nat) (c : Stored) (b : List Stored)
    (hnow : gcBefore ≤ now) :
    ferrosaRead now gcBefore top [c] b = readAt now (c :: b) := by
  unfold ferrosaRead strippedResponse compactCells
  by_cases hd : droppableRepaired gcBefore (replicaGuard gcBefore top [c]) c = true
  · have hp : pastGrace gcBefore c = true := by
      have h := hd
      rw [droppable_iff_past_grace_below_guard] at h
      simp at h
      exact h.1
    by_cases hg : c.obs.ts < replicaGuard gcBefore top b
    · simp only [List.filter_cons, List.filter_nil, hd, if_true, List.all_cons, List.all_nil,
        decide_eq_true hg, Bool.and_true, Bool.not_true, Bool.false_eq_true, if_false,
        List.nil_append]
      exact (a_stripped_version_below_every_other_guard_leaves_the_read_unchanged
        now gcBefore top c b hp hg hnow).symm
    · simp only [List.filter_cons, List.filter_nil, hd, if_true, List.all_cons, List.all_nil,
        decide_eq_false hg, Bool.false_and, Bool.false_eq_true, if_false, List.singleton_append]
  · have hd' : droppableRepaired gcBefore (replicaGuard gcBefore top [c]) c = false := by
      cases h : droppableRepaired gcBefore (replicaGuard gcBefore top [c]) c
      · rfl
      · exact absurd h hd
    simp only [List.filter_cons, List.filter_nil, hd', Bool.false_eq_true, if_false,
      List.all_nil, if_true, Bool.not_false, List.singleton_append]

/-- Cassandra's `DataLimits` row counter: every row passes through, only a visible row counts, and it
    stops once `n` have. -/
def liveLimit (now : Nat) : Nat → List Stored → List Stored
  | 0, _ => []
  | _, [] => []
  | n + 1, r :: rs => if r.visibleAt now then r :: liveLimit now n rs else r :: liveLimit now (n + 1) rs

/-- **A limit that counts live rows returns the same live rows from a stripped page.** Every row the
    strip drops is past grace and so invisible, and an invisible row never counts toward the limit. -/
theorem a_live_row_limit_keeps_its_live_rows_after_stripping (now gcBefore : Nat)
    (hnow : gcBefore ≤ now) (n : Nat) (rows : List Stored) :
    (liveLimit now n (rows.filter (fun r => !pastGrace gcBefore r))).filter (fun r => r.visibleAt now)
      = (liveLimit now n rows).filter (fun r => r.visibleAt now) := by
  induction rows generalizing n with
  | nil => cases n <;> simp [liveLimit]
  | cons r rs ih =>
    cases n with
    | zero => simp [liveLimit]
    | succ m =>
      by_cases hp : pastGrace gcBefore r = true
      · have hv : r.visibleAt now = false := past_grace_is_invisible now gcBefore r hp hnow
        simp [hp, liveLimit, hv, ih]
      · by_cases hv : r.visibleAt now = true
        · simp [hp, liveLimit, hv, ih]
        · simp [hp, liveLimit, hv, ih]

/-- **A limit that counts raw rows returns different rows from a stripped page.** `LIMIT 1` over an
    expired row followed by a live one returns nothing visible. The same limit over the stripped page
    returns the live row, so the page boundary and the caller's resume point move. -/
theorem a_raw_row_limit_moves_when_a_page_is_stripped :
    List.filter (fun r => r.visibleAt 100)
        (([⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨11, false, 2⟩, none⟩] : List Stored).take 1) = []
      ∧ List.filter (fun r => r.visibleAt 100)
          ((([⟨⟨10, false, 1⟩, some 20⟩, ⟨⟨11, false, 2⟩, none⟩] : List Stored).filter
            (fun r => !pastGrace 30 r)).take 1) = [⟨⟨11, false, 2⟩, none⟩] := by
  decide

/-! ## The ledger

Every claim about deletes and expiry is listed here with the theorem that supports it, and the four red items
are listed with equal prominence: commutativity is checked on the colliding cases rather than proved in
general, a stored `i32` deletion time wraps, the purge path checks neither precondition, and `gc_grace` has no
purge path behind it. A reader of the table can see exactly what a delete promises today and where the promise
stops. The count is pinned, so closing a red item without a table update fails the build.
-/

def ledger : List SpecItem := [
  ⟨"a newer write beats an older one",
   .met "Tombstone.merge_prefers_newer"⟩,
  ⟨"a tombstone beats a value at equal timestamp",
   .met "Tombstone.merge_tombstone_wins_tie"⟩,
  ⟨"the merge is order-independent",
   .failing
     "commutativity is checked on the colliding cases, not proved in general; the general statement needs a total order on (ts, tombstone, value)"
     "Tombstone.ts_only_merge_does_not_converge"⟩,
  ⟨"TTL does not depend on a replica's clock",
   .met "Tombstone.expiry_depends_only_on_the_stored_ldt"⟩,
  ⟨"a TTL cell expires exactly at its local deletion time",
   .met "Tombstone.ttl_expires_exactly_at_ldt"⟩,
  ⟨"a cell with a past USING TIMESTAMP expires on arrival",
   .met "Tombstone.past_timestamp_expires_on_arrival"⟩,
  ⟨"a local deletion time is representable for every valid TTL",
   .failing
     "cell ldt is an i32: any TTL pushing it past 2^31 seconds wraps negative and reads as already expired; a ~12-year TTL from today is enough"
     "Tombstone.ldt_overflows_before_2038_reachable_now"⟩,
  ⟨"a tombstone shadows every value at or below its timestamp, so no arrival order resurrects the row",
   .met "Tombstone.a_tombstone_shadows_every_value_at_or_below_its_timestamp"⟩,
  ⟨"purging requires both the grace window and propagation to every replica",
   .met "Tombstone.purging_needs_the_grace_window_and_propagation"⟩,
  ⟨"purging on the grace window alone lets a stale replica resurrect the row",
   .met "Tombstone.purging_before_propagation_lets_a_stale_value_win"⟩,
  ⟨"deleted data cannot be resurrected",
   .failing
     "dominance holds in any order, but the purge path does not check either condition, so today the property rests on there being no purge at all. Purging before propagation resurrects"
     "Tombstone.purge_before_propagation_resurrects"⟩,
  ⟨"purge is safe once every replica applied the tombstone",
   .met "Tombstone.purge_safe_after_all_replicas_applied"⟩,
  ⟨"a purge that keeps every value-bearing cell keeps expired TTL history forever",
   .met "Tombstone.expired_history_grows_without_bound"⟩,
  ⟨"compaction past the grace window and the overlap guard removes expired TTL data",
   .met "Tombstone.repaired_compaction_drops_expired_history"⟩,
  ⟨"dropping a purgeable tombstone or expired cell is invisible to every later read",
   .met "Tombstone.purging_expired_data_is_invisible_to_reads"⟩,
  ⟨"dropping expired data that older data could sit under resurrects that data",
   .met "Tombstone.dropping_expired_data_above_the_guard_resurrects"⟩,
  ⟨"rewriting expired data as a tombstone changes an equal-timestamp read, so it is dropped instead",
   .met "Tombstone.rewriting_expired_data_as_a_tombstone_changes_a_tie"⟩,
  ⟨"replicas that differ only by data one of them already purged compute the same read digest",
   .met "Tombstone.replicas_differing_only_by_purgeable_data_agree"⟩,
  ⟨"a repair that writes the merged winner to every replica stores purged data no read can see",
   .met "Tombstone.shipped_repair_writes_purged_data_back"⟩,
  ⟨"read repair leaves every target reading the coordinator's merged answer, whether or not it sends purgeable data",
   .met "Tombstone.repair_converges_every_replica_to_the_merged_answer"⟩,
  ⟨"leaving purgeable data out of a repair write does only what the target's own compaction does",
   .met "Tombstone.repaired_write_matches_the_shipped_write_after_compaction"⟩,
  ⟨"a repair must still send purgeable data that shadows a live, older value on the target",
   .met "Tombstone.skipping_a_repair_over_older_live_data_resurrects"⟩,
  ⟨"replicas whose read digests match share the repair guard of the full-read replica",
   .met "Tombstone.replicas_with_equal_digests_share_the_guard"⟩,
  ⟨"a full-read response that ships every version carries purged data no read can see",
   .met "Tombstone.shipped_response_carries_purged_data"⟩,
  ⟨"a response stripped of what the replica's compaction would drop gives the coordinator the same repair guard",
   .met "Tombstone.stripping_keeps_the_replica_guard"⟩,
  ⟨"a version left out of a response below every other responder's guard changes no merged read",
   .met "Tombstone.a_stripped_version_below_every_other_guard_leaves_the_read_unchanged"⟩,
  ⟨"merging a stripped response over older live data on another responder resurrects it, so the coordinator re-reads in full",
   .met "Tombstone.keeping_a_stripped_response_over_older_live_data_resurrects"⟩,
  ⟨"a response whose digest matched every other responder passes the stripped-response check",
   .met "Tombstone.a_matching_digest_passes_the_stripped_response_check"⟩,
  ⟨"Cassandra's guard-less read-path strip changes no read on the replica that strips",
   .met "Tombstone.cassandra_response_reads_like_the_replica"⟩,
  ⟨"Cassandra's unchecked merge of stripped responses is correct when nothing live on another replica sits under the dropped version",
   .met "Tombstone.cassandra_read_is_correct_when_nothing_live_sits_under_the_dropped_version"⟩,
  ⟨"without repair inside gc_grace, Cassandra's read resurrects an older value and Ferrosa's checked read does not",
   .met "Tombstone.cassandra_read_without_propagation_resurrects"⟩,
  ⟨"Ferrosa's checked read of a stripped response equals the full read with no repair assumption",
   .met "Tombstone.ferrosa_read_needs_no_propagation"⟩,
  ⟨"a limit counting live rows, as Cassandra's does, returns the same live rows from a stripped page",
   .met "Tombstone.a_live_row_limit_keeps_its_live_rows_after_stripping"⟩,
  ⟨"a limit counting raw rows, as Ferrosa's internode row_limit does, returns different rows from a stripped page",
   .met "Tombstone.a_raw_row_limit_moves_when_a_page_is_stripped"⟩,
  ⟨"the purge path checks the grace window and propagation before removing a tombstone",
   .failing
     "gc_grace is a display default with no purge path behind it, so the precondition is never checked"
     "Tombstone.purge_requires_all_replicas_applied"⟩
]

/-- Four obligations are recorded red: a general proof that the merge is order-independent, an `i32`
    local deletion time that survives every valid TTL, a delete that cannot be resurrected, and a
    purge path that checks the grace window and propagation before removing anything. The count is
    pinned so none of them can leave the ledger unnoticed. -/
theorem tombstone_has_four_red_items : redCount ledger = 4 := by decide

def main : IO Unit :=
  printLedger "Tombstone, TTL and purge obligations" ledger

end Ferrosa.Storage.Tombstone

